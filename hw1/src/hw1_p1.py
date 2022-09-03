import logging
import argparse
import multiprocessing
import random

NUM_OF_FILES = 2
NUM_OF_LINES = 10
FILE_PREFIX = "cscie88_fall2022"

logger = logging.getLogger(__name__)


def generate_file(num_lines: int,
                  file_number: int,
                  ) -> None:
    """
    A method to generate a file with the specified number of lines, each with 3 random numbers
    Args:
        num_lines: the required number of lines to be written in the file
        file_number: The sequential number of file that needs to be generated
    """

    filename = f"{FILE_PREFIX}_{file_number}.txt"

    with open(filename, "w") as file:
        for _ in range(num_lines):
            r1 = random.randint(0, 10)
            r2 = random.randint(0, 10)
            r3 = random.randint(0, 10)
            file.write(f"{r1} {r2} {r3}\n")

        logger.info("file written", extra={"file_name": filename})


def parse_arguments() -> argparse.Namespace:
    """
    A method to parse generated program arguments

    Returns: argparse.Namespace object with two arguments
    "num_files": required number of files to generate
    "num_lines": required number of lines to generate in each file
    """
    parser = argparse.ArgumentParser(description='Set the number of files and number of lines')

    parser.add_argument("num_files", nargs='?', type=int, help="Number of files to create", default=NUM_OF_FILES)
    parser.add_argument("num_lines", nargs='?', type=int, help="Number of lines per file", default=NUM_OF_LINES)

    args = parser.parse_args()

    logger.info("Program args recorded", extra={"num_files": args.num_files, "num_lines": args.num_lines})

    return args


def generate_files(num_files, num_lines) -> None:
    """
    A method to generate the required amount of files, each on a separate thread
    Args:
        num_files: required number of files to generate
        num_lines: required number of lines to generate in each file
    """
    jobs = []

    # Generate file in separate threads

    for file_number in range(num_files):
        t = multiprocessing.Process(target=generate_file, args=(num_lines, file_number))
        jobs.append(t)
        t.start()
        logger.info("Job launched", extra={"file_number": file_number})

    for curr_job in jobs:
        curr_job.join()

    logger.info("All threads completed successfully")


def main():
    """
    A wrapper method to generate required files.
    Files will be generated to the working directory of the program invoker
    """
    logger.info("Parsing arguments")

    arguments = parse_arguments()
    generate_files(arguments.num_files, arguments.num_lines)

    logger.info("Program successfully completed")
