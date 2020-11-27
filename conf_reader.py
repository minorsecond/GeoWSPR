"""
Read configuration data from config file.
"""

import configparser


def read_config():
    """
    Read the config file & pass settings on.
    :return: A tuple of settings.
    """

    config = configparser.ConfigParser()
    config.read("config")

    db_user = config['DB']['db_user']
    db_pass = config['DB']['db_pass']
    db_host = config['DB']['db_host']
    db_name = config['DB']['db_name']

    print("DB Configuration:")
    print(f"DB User: {db_user}")
    print(f"DB Password: {db_pass}")
    print(f"DB Host: {db_host}")
    print(f"DB Name: {db_name}")
    print("-----------------------")

    return (db_user, db_pass, db_host, db_name)
