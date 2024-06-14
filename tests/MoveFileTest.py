import os
import tempfile
import unittest
from multiprocessing import Lock
from unittest.mock import MagicMock

from tasks.FileMover import check_file_exists, check_file_does_not_exists, create_if_not_exists, get_lock_file_path, \
    read_file, write_to_file, backup_file, restore_file, move, delete_files, move_file, release_locks


class TestFileOperations(unittest.TestCase):

    def test_move_file(self):
        with tempfile.TemporaryDirectory() as tempdir:
            src_file = os.path.join(tempdir, 'source.txt')
            dest_dir = os.path.join(tempdir, 'dest')
            dest_file = os.path.join(dest_dir, 'source.txt')
            with open(src_file, 'w') as f:
                f.write('Random text.')

            move_file(src_file, dest_dir, Lock())

            self.assertFalse(os.path.exists(src_file))
            self.assertTrue(os.path.exists(dest_file))
            with open(dest_file, 'r') as f:
                self.assertEqual(f.read(), 'Random text.')

    def test_check_file_exists_true(self):
        with tempfile.NamedTemporaryFile() as temp_file:
            check_file_exists(temp_file.name)

    def test_check_file_exists_false(self):
        with self.assertRaises(FileNotFoundError):
            check_file_exists('random_name.txt')

    def test_check_file_does_not_exists_false(self):
        with tempfile.NamedTemporaryFile() as temp_file:
            with self.assertRaises(FileExistsError):
                check_file_does_not_exists(temp_file.name)

    def test_check_file_does_not_exists_true(self):
        check_file_does_not_exists('random_name.txt')

    def test_create_if_not_exists(self):
        with tempfile.TemporaryDirectory() as tempdir:
            dest = os.path.join(tempdir, 'dest')
            create_if_not_exists(dest)
            self.assertTrue(os.path.exists(dest))

    def test_get_lock_file_path(self):
        self.assertEqual('test.csv.lock', get_lock_file_path('test.csv'))

    def test_read_file(self):
        with tempfile.NamedTemporaryFile() as temp_file:
            temp_file.write(b'Random text')
            temp_file.seek(0)
            content = read_file(temp_file.name)
            self.assertEqual(b'Random text', content)

    def test_write_to_file(self):
        with tempfile.NamedTemporaryFile() as temp_file:
            write_to_file(temp_file.name, b'Random text')
            with open(temp_file.name, 'rb') as f:
                self.assertEqual(b'Random text', f.read())

    def test_backup_file(self):
        with tempfile.NamedTemporaryFile() as temp_file:
            temp_file.write(b'Random text')
            temp_file.seek(0)
            backup = backup_file(temp_file.name)
            self.assertEqual(b'Random text', backup)

    def test_restore_file(self):
        with tempfile.NamedTemporaryFile() as temp_file:
            restore_file(temp_file.name, b'Random text')
            with open(temp_file.name, 'rb') as f:
                self.assertEqual(b'Random text', f.read())

    def test_move(self):
        with tempfile.TemporaryDirectory() as tempdir:
            with tempfile.NamedTemporaryFile() as source:
                destination = os.path.join(tempdir, 'dest.txt')

                move(source.name, destination)

                self.assertFalse(os.path.exists(source.name))
                self.assertTrue(os.path.exists(destination))

    def test_delete_files(self):
        with tempfile.NamedTemporaryFile() as temp_file:
            delete_files([temp_file.name])
            self.assertFalse(os.path.exists(temp_file.name))

    def test_release_locks(self):
        lock = MagicMock()
        lock.is_locked = True
        release_locks([lock])

        lock.release.assert_called_once()

