import asyncio
import csv
import tempfile
import unittest

import aiofiles
import redis
from testcontainers.redis import RedisContainer

from tasks.FileMerger import FileMerger


class TestFileMerger(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.redis_container = RedisContainer()
        cls.redis_container.start()
        cls.redis_host = cls.redis_container.get_container_host_ip()
        cls.redis_port = cls.redis_container.get_exposed_port(6379)
        cls.redis_client = redis.Redis(host=cls.redis_host, port=cls.redis_port)

    @classmethod
    def tearDownClass(cls):
        cls.redis_container.stop()

    def setUp(self):
        self.file_merger = FileMerger(redis_host=self.redis_host, redis_port=self.redis_port)
        self.redis_client.flushall()

    def test_process(self):
        async def run_test():
            with tempfile.NamedTemporaryFile() as temp_input:
                temp_input.write(b'Adam 1234\nJohn 4321\n')
                temp_input.seek(0)

                await self.file_merger.process(temp_input.name, 'First', chunk_size=2)

            self.assertIn(b'First', self.file_merger.redis.hgetall('4321'))
            self.assertIn(b'First', self.file_merger.redis.hgetall('1234'))
            name = self.file_merger.redis.hgetall('1234').get(b'First')
            self.assertEqual(b'Adam', name)

        asyncio.run(run_test())

    def test_read_chunk(self):
        async def run_test():
            with tempfile.NamedTemporaryFile() as temp_input:
                temp_input.write(b'Adam 1234\nJohn 4321\nJosh 5541\n')
                temp_input.seek(0)

                async with aiofiles.open(temp_input.name, 'r') as file:
                    lines = await FileMerger.read_chunk(file, 2)
                    self.assertEqual(len(lines), 2)
                    self.assertEqual(lines, ['Adam 1234', 'John 4321'])

        asyncio.run(run_test())

    def test_validate_lines(self):
        lines = ['Adam 1234', 'John 4321']
        valid_lines = FileMerger.validate_lines(lines)
        self.assertEqual(valid_lines, [('1234', 'Adam'), ('4321', 'John')])

    def test_process_data(self):
        lines = ['Adam 1234', 'John 4321']
        self.file_merger.process_data(lines, 'First')
        self.assertEqual(self.file_merger.redis.hget('1234', 'First'), b'Adam')
        self.assertEqual(self.file_merger.redis.hget('4321', 'First'), b'John')

    def test_get_ids(self):
        self.file_merger.redis.zadd('sorted_ids', {'1': 1, '10': 10, '2': 2})
        ids = self.file_merger.get_ids(0, 2)
        self.assertEqual(ids, ['1', '2'])

    def test_get_data_by_ids(self):
        self.file_merger.redis.hset('1234', mapping={'First': 'Adam', 'Last': 'Johnson'})
        self.file_merger.redis.hset('4321', mapping={'First': 'John', 'Last': 'Adamson'})
        self.file_merger.redis.hset('5555', mapping={'First': 'John', 'Last': 'Johnson'})
        ids = ['1234', '4321']
        data = self.file_merger.get_data_by_ids(ids)
        self.assertEqual(data, [{'ID': '1234', 'First': 'Adam', 'Last': 'Johnson'}, {'ID': '4321', 'First': 'John', 'Last': 'Adamson'}])

    def test_format_data(self):
        ids = ['1234']
        responses = [{b'First': b'Adam', b'Last': b'Johnson'}]
        formatted_data = FileMerger.format_data(ids, responses)
        self.assertEqual(formatted_data, [{'ID': '1234', 'First': 'Adam', 'Last': 'Johnson'}])

    def test_write_to_csv(self):
        with tempfile.NamedTemporaryFile(mode='w+', newline='') as temp_output:
            writer = csv.writer(temp_output, delimiter=' ')
            data = [{'ID': '1234', 'First': 'John', 'Last': 'Adamson'}]
            FileMerger.write_to_csv(writer, data)
            temp_output.seek(0)
            rows = temp_output.readlines()
            self.assertEqual(1, len(rows))
            self.assertEqual( "John Adamson 1234", rows[0].strip())
