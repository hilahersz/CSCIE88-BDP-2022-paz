import logging
import argparse
import multiprocessing
import random

NUM_OF_FILES = 2
NUM_OF_LINES = 10
FILE_PREFIX = "cscie88_fall2022"

logger = logging.getLogger(__name__)


def generate_file(num_lines,
                  file_number) -> None:
    """
    Function to generate a file with the specified number of lines, each with 3 random numbers
    """

    filename = f"{FILE_PREFIX}_{file_number}.txt"

    with open(filename, "w") as file:
        for _ in range(num_lines):
            r1 = random.randint(0, 10)
            r2 = random.randint(0, 10)
            r3 = random.randint(0, 10)
            file.write(f"{r1} {r2} {r3}\n")

        logger.info("file written", extra={"file_name": filename})


def parse_arguments():
    parser = argparse.ArgumentParser(description='Set the number of files and number of lines')
    parser.add_argument("num_files", nargs='?', type=int, help="Number of files to create", default=NUM_OF_FILES)
    parser.add_argument("num_lines", nargs='?', type=int, help="Number of lines per file", default=NUM_OF_LINES)
    args = parser.parse_args()

    return args


def main():
    logger.info("Parsing arguments")
    arguments = parse_arguments()
    num_files = arguments.num_files
    num_lines = arguments.num_lines
    logger.info("Program args recorded", extra={"num_files": num_files, "num_lines": num_lines})

    jobs = []
    for file_number in range(num_files):
        t = multiprocessing.Process(target=generate_file, args=(num_lines, file_number))
        jobs.append(t)
        t.start()
        logger.info("Job launched", extra={"file_number": file_number})

    for curr_job in jobs:
        curr_job.join()

    logger.info("Program successfully completed")
