import gzip
import os
import re
import shutil
import urllib.request

import requests
from bs4 import BeautifulSoup

"""
Downloads and extracts WSPR data from the internet.
"""


def get_download_urls():
    """
    Get list of download URLS for WSPR data
    :return: list of URLS
    """

    page = requests.get("http://wsprnet.org/drupal/downloads").text
    soup = BeautifulSoup(page, 'html.parser')
    links = []
    pattern = re.compile(r"http://wsprnet.org/archive/.*.csv.gz")

    for a in soup.findAll('a', href=pattern):
        links.append(a['href'])

    return links


def download_menu(url_list):
    """
    Presents user with menu of file(s) to download
    :return:
    """

    print("Select WSPR data file(s) to download by entering the ID(s) or 0 for all. \nNote: the current month's data "
          "may not yet be complete\n\n")

    list_id = 0

    print("ID\tURL")
    for url in url_list:
        list_id += 1

        print(f"{list_id}:\t{url}")

    download_selection = input("IDs to download (0 for all): ")

    # Remove whitespaces
    download_selection = download_selection.strip()
    download_selection = download_selection.replace(" ", ",")
    download_selection = download_selection.split(",")

    if '0' not in download_selection:
        selections = []
        for selection in download_selection:
            selections.append(url_list[int(selection) - 1])

    else:
        selections = url_list

    return selections


def extract_gzip(input_file, output_path):
    """
    Extracts gzip WSPR files
    :param input_file: path to compressed files
    :param output_path: output path
    :return: None
    """

    print(f"Extracting {input_file} to {output_path}")
    with gzip.open(input_file, 'rb') as f_in:
        with open(output_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    os.remove(input_file)


def download_wspr_data(wanted_urls):
    """
    Download selected URLs
    :param wanted_urls: List containing selected URLs
    :return:
    """

    download_directory = input("Enter download directory: ")
    if not os.path.exists(download_directory):
        os.makedirs(download_directory)

    for download_file in wanted_urls:
        print(f"Downloading {download_file}")
        filename = os.path.basename(download_file)
        extracted_filename = os.path.splitext(filename)[0]
        download_path = os.path.join(download_directory, filename)
        output_path = os.path.join(download_directory, extracted_filename)

        if not os.path.exists(download_path) and not os.path.exists(output_path):  # Download & extract
            urllib.request.urlretrieve(download_file, filename=download_path)
            extract_gzip(download_path, output_path)

        elif os.path.exists(download_path) and not os.path.exists(output_path):  # Already downloaded - extract
            extract_gzip(download_path, output_path)

        elif os.path.exists(output_path) and os.path.exists(download_path):  # Remove compressed file
            print("Both compressed file & CSV file exist. Removing compressed file")
            os.remove(download_path)

        # File has previously been processed. Continue, doing nothing in this iteration
        elif os.path.exists(output_path) and not os.path.exists(download_path):
            print(f"{output_path} already exists. Skipping.")

    print("Downloading complete")


def main():
    """
    Runs the thing
    :return:
    """

    file_urls = get_download_urls()
    wanted_urls = download_menu(file_urls)
    download_wspr_data(wanted_urls)


main()
