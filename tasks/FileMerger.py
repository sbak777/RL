import asyncio
import csv
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import List, Tuple, Dict

import aiofiles
import redis

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FileMerger:
    def __init__(self, redis_host="127.0.0.1", redis_port=6379, redis_db=0, max_workers=20):
        self.redis = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
        self.executor = ThreadPoolExecutor(max_workers=max_workers)

    async def process(self, path: str, key: str, chunk_size: int = 100) -> None:
        try:
            async with aiofiles.open(path, 'r') as file:
                while True:
                    lines = await self.read_chunk(file, chunk_size)
                    if not lines:
                        break
                    await asyncio.to_thread(self.process_data, lines, key)
        except Exception as e:
            logger.error(f"An error occurred while processing the file {path}: {e}")

    @staticmethod
    async def read_chunk(file, chunk_size: int) -> List[str]:
        lines = []
        async for line in file:
            lines.append(line.strip())
            if len(lines) >= chunk_size:
                break
        return lines

    def process_data(self, lines: List[str], key: str) -> None:
        try:
            pipeline = self.redis.pipeline()
            valid_lines = self.validate_lines(lines)
            for id_, value in valid_lines:
                pipeline.hset(id_, mapping={key: value})
                pipeline.zadd('sorted_ids', {id_: id_})
            pipeline.execute()
        except Exception as e:
            logger.error(f"An error occurred while processing data: {e}")

    @staticmethod
    def validate_lines(lines: List[str]) -> List[Tuple[str, str]]:
        valid_lines = []
        for line in lines:
            parts = line.split()
            if len(parts) != 2:
                logger.warning(f"Invalid line, skipped: {line}")
                continue
            valid_lines.append((parts[1], parts[0]))
        return valid_lines

    def print_result_file(self, result_file: str, chunk_size: int = 200):
        start = 0
        try:
            with open(result_file, 'a') as file:
                writer = csv.writer(file, delimiter=' ')
                while True:
                    ids = self.get_ids(start, chunk_size)
                    if not ids:
                        break
                    data = self.get_data_by_ids(ids)
                    self.write_to_csv(writer, data)
                    start += chunk_size
        except Exception as e:
            logger.error(f"An error occurred while exporting file {result_file}: {e}")

    def get_ids(self, start: int = 0, chunk_size: int = 200) -> List[str]:
        ids = self.redis.zrange('sorted_ids', start, start + chunk_size - 1)
        return [id_.decode('utf-8') for id_ in ids]

    def get_data_by_ids(self, ids: List[str]) -> List[Dict[str, str]]:
        pipeline = self.redis.pipeline()
        for id_ in ids:
            pipeline.hgetall(id_)

        return self.format_data(ids, pipeline.execute())

    @staticmethod
    def format_data(ids: List[str], responses: List[Dict[bytes, bytes]]) -> List[Dict[str, str]]:
        results = []
        for id_, response in zip(ids, responses):
            if response:
                name = response.get(b'First', b'').decode('utf-8')
                surname = response.get(b'Last', b'').decode('utf-8')
                results.append({'ID': id_, 'First': name, 'Last': surname})
            else:
                results.append({'ID': id_, 'First': 'No data found', 'Last': 'No data found'})
        return results

    @staticmethod
    def write_to_csv(writer, data: List[Dict[str, str]]) -> None:
        rows = [[item['First'], item['Last'], item['ID']] for item in data]
        writer.writerows(rows)

    async def main(self, first_names_file, last_names_file, result_file, chunk_size=100, writer_chunk_size=200):
        first_names_task = asyncio.create_task(self.process(first_names_file, 'First', chunk_size))
        last_names_task = asyncio.create_task(self.process(last_names_file, 'Last', chunk_size))
        await asyncio.gather(first_names_task, last_names_task)
        self.print_result_file(result_file, writer_chunk_size)


if __name__ == "__main__":
    merger = FileMerger()
    asyncio.run(merger.main("names.txt", "surnames.txt", "result.txt", 100, 200))
