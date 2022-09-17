import argparse

from .q1 import do_work


def parse_arguments() -> argparse.Namespace:
    """
    A method to parse arguments for q1 b. all arguments have default values
    Returns: a parsed_argument object with the following attributes:
        redis_counter_name: a name to the redis counter
        file_path: a data folder with all .csv files to process
        redis_url: where to locate the redis server
    """
    prog = "counter_process_redis"
    desc = "application that reads a file, parses all lines, counts the lines and " \
           "stores/increments the counter maintained in Redis"

    parser = argparse.ArgumentParser(prog=prog, description=desc)
    # name of a simple String field in Redis - that will be use as a shared counter
    parser.add_argument('--redis_counter_name', '-rc', required=False, default="counter")
    parser.add_argument('--file_path', '-f', required=False, default="src/logs",
                        help="a csv log file to process")
    parser.add_argument('--redis_url', '-ru', required=False, default="redis://localhost:6379",
                        help="Redis end point url; Eg: redis://localhost:6379")

    parsed_args = parser.parse_args()
    return parsed_args


def main():
    """
    A wrapper method to execute the redis distinct count operations for file-inputs 1,2,3 and 4.
    Returns:

    """
    parsed_args = parse_arguments()
    redis_counter_name = parsed_args.redis_counter_name
    file_path = parsed_args.file_path
    redis_url = parsed_args.redis_url
    for i in [1, 2, 3, 4]:
        file_name = f"{file_path}/file-input{i}.csv"
        do_work(redis_url, redis_counter_name, file_name)
