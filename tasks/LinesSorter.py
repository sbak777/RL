import logging
from collections import Counter
from typing import List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LinesSorter:

    @staticmethod
    def sort_letters(word: str) -> str:
        #O(n)
        char_dict = LinesSorter.create_counter(word)
        #O(nlogn), n=26 -> O(1)
        sorted_characters = LinesSorter.sort_characters(char_dict)

        return ''.join(sorted_characters)

    @staticmethod
    def create_counter(word: str) -> Counter:
        return Counter(word)

    @staticmethod
    def sort_characters(char_dict: Counter) -> List[str]:
        # O = O(klogk), k = 26 -> O(1)
        sorted_keys = sorted(char_dict.keys())
        sorted_characters = []
        for key in sorted_keys:
            sorted_characters.extend(key * char_dict[key])

        return sorted_characters

    @staticmethod
    def sort_lines(file_path: str) -> List[str]:
        sorted_lines = []
        try:
            with open(file_path, 'r') as file:
                for line in file:
                    sorted_lines.append(LinesSorter.sort_letters(line.strip()))
            # O(n log n), where n is not large
            return sorted(sorted_lines)
        except FileNotFoundError:
            logging.error(f"File not found error: {file_path}")
        except Exception as e:
            logging.error(f"An error occurred while reading the file '{file_path}': {e}")

    @staticmethod
    def write_to_file(lines: List[str], output_file_path: str) -> None:
        try:
            with open(output_file_path, 'w') as file:
                for line in lines:
                    file.write(line + '\n')
            logging.info(f"Sorted line exported into '{output_file_path}'")
        except Exception as e:
            logging.error(f"An error occurred while writing to the file '{output_file_path}': {e}")



if __name__ == "__main__":
    LinesSorter.write_to_file(LinesSorter.sort_lines("../../../long_words.txt"), "../../../output3.txt")
