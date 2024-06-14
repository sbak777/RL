import tempfile
import unittest
from collections import Counter

from tasks.LinesSorter import LinesSorter


class TestLinesSorter(unittest.TestCase):

    def test_sort_letters(self):
        self.assertEqual(LinesSorter.sort_letters("dbca"), "abcd")
        self.assertEqual(LinesSorter.sort_letters("dddca"), "acddd")

    def test_create_counter(self):
        counter = LinesSorter.create_counter("aabbcc")
        self.assertEqual(Counter({'a': 2, 'b': 2, 'c': 2}), counter)

    def test_sort_characters(self):
        counter = Counter({'c': 2, 'b': 2, 'a': 2})
        sorted_characters = LinesSorter.sort_characters(counter)
        self.assertEqual(['a', 'a', 'b', 'b', 'c', 'c'], sorted_characters)

    def test_sort_lines(self):
        with tempfile.NamedTemporaryFile(mode='w+') as temp_file:
            temp_file.write("this\nis\nan\nexample")
            temp_file.seek(0)

            sorted_lines = LinesSorter.sort_lines(temp_file.name)
            self.assertEqual(sorted_lines, ["aeelmpx", "an", "hist", "is"])

    def test_write_to_file(self):
        data = ["aeelmpx", "an", "hist", "is"]
        with tempfile.NamedTemporaryFile(mode='w+') as temp_file:
            LinesSorter.write_to_file(data, temp_file.name)
            content = temp_file.readlines()
            self.assertEqual(content, ["aeelmpx\n", "an\n", "hist\n", "is\n"])
