import argparse
import logging
import os
import shutil
from multiprocessing import Lock
from typing import List

from filelock import FileLock, Timeout

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def move_file(file_path: str, dest_folder: str, lock: Lock):
    check_file_exists(file_path)
    dest_path = validate_destination_path(dest_folder, file_path)

    source_lock_file_path = get_lock_file_path(file_path)
    dest_lock_file_path = get_lock_file_path(dest_path)

    logger.info(f"Moving file {file_path} to {dest_folder}")

    with lock:
        source_lock = FileLock(source_lock_file_path)
        dest_lock = FileLock(dest_lock_file_path)
        try:
            logger.info("Acquiring locks for files.")
            with source_lock.acquire(timeout=10):
                logger.info(f"Lock acquired for: {file_path}")
                try:
                    backup = backup_file(file_path)

                    with dest_lock.acquire(timeout=10):
                        logging.info(f"Destination lock acquired for {file_path}")

                        try:
                            shutil.move(file_path, dest_path)
                            print(dest_path)
                            logging.info(f"Successfully moved {file_path} to {dest_path}")
                        except Exception as e:
                            logging.error(f"Failed to move {file_path}: {e}")
                            restore_file(file_path, backup)

                except Exception as e:
                    logging.error(f"Failed to backup/restore {file_path}: {e}")

        except Timeout:
            logging.error(f"Timeout while trying to acquire lock for {file_path}")
        finally:
            release_locks([source_lock, dest_lock])
            delete_files([source_lock_file_path, dest_lock_file_path])


def check_file_exists(file_path: str) -> None:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File {file_path} does not exist.")


def check_file_does_not_exists(file_path: str) -> None:
    if os.path.exists(file_path):
        raise FileExistsError(f"File {file_path} already exists.")


def create_if_not_exists(destination_directory: str) -> None:
    if not os.path.exists(destination_directory):
        os.makedirs(destination_directory)


def get_lock_file_path(file_path: str) -> str:
    return file_path + '.lock'


def read_file(source_path: str) -> bytes:
    with open(source_path, 'rb') as f:
        return f.read()


def write_to_file(source_path: str, content: bytes) -> None:
    with open(source_path, 'wb') as f:
        f.write(content)


def backup_file(file_path: str) -> bytes:
    try:
        return read_file(file_path)
    except Exception as e:
        logging.error(f"Failed to backup file {file_path}: {e}")
        raise Exception(f"Failed to backup file {file_path}: {e}")


def restore_file(file_path: str, content: bytes) -> None:
    try:
        write_to_file(file_path, content)
        logging.info(f"Restored {file_path} after failure.")
    except Exception as e:
        logging.critical(f"Failed to restore {file_path} to source: {e}")
        raise Exception(f"Failed to restore {file_path} to source: {e}")


def move(source_path: str, destination_path: str) -> None:
    try:
        shutil.move(source_path, destination_path)
        logging.info(f"Successfully moved {source_path} to {destination_path}")
    except Exception as e:
        logging.error(f"Failed to move {source_path}: {e}")
        raise Exception(f"Failed to move {source_path}: {e}")


def delete_files(files: List[str]) -> None:
    for file in files:
        if os.path.exists(file):
            os.remove(file)


def release_locks(locks) -> None:
    for lock in locks:
        if lock.is_locked:
            lock.release()
            logging.info("Lock released.")


def validate_destination_path(dest_folder: str, file_path: str) -> str:
    create_if_not_exists(dest_folder)
    dest_path = os.path.join(dest_folder, os.path.basename(file_path))
    check_file_does_not_exists(dest_path)

    return dest_path


def main(source_file: str = None, dest_folder: str = None) -> None:
    if source_file is not None and dest_folder is not None:
        parser = argparse.ArgumentParser()
        parser.add_argument('source_file', type=str)
        parser.add_argument('dest_folder', type=str)
        args = parser.parse_args()
        source_file = args.source_file
        dest_folder = args.dest_folder

    move_file(source_file, dest_folder, Lock())


if __name__ == "__main__":
    main("/Users/Shared/Workspace/reconciliation-report-2024-04-09.csv", "/Users/Shared/Workspace/dest/")
    #main()
