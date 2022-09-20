from .q1 import do_work, parse_arguments


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
