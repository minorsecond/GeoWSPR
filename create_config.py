"""
Creates the configuration file.
"""


def prompt():
    """
    Prompts the user for question.
    :return: Dict containing responses.
    """

    responses = {"db_user": input("Database username: "),
                 "db_pass": input("Database password: "),
                 "db_host": input("Database host IP: "),
                 "db_name": input("Database name: ")}

    return responses


def write_to_file(responses):
    """
    Writes the responses to txt file.
    :param responses: Dict of responses.
    """

    f = open("config", "w+")
    f.write("[DB]\n")
    f.write(f"db_user = {responses['db_user']}\n")
    f.write(f"db_pass = {responses['db_pass']}\n")
    f.write(f"db_host = {responses['db_host']}\n")
    f.write(f"db_name = {responses['db_name']}\n")


def main():
    """
    Run the script.
    """
    responses = prompt()
    write_to_file(responses)


if __name__ == '__main__':
    main()
