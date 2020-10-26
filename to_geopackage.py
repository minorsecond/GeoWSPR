import csv
import os

import fiona
import psycopg2
import configparser
from fiona.crs import from_string

"""
Creates a geopackage file out of the CSV files downloaded using the download.py script
"""

# TODO: Migrate to geoalchemy

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

# WGS84 Projection￿
crs = from_string('+proj=longlat +ellps=WGS84 +datum=WGS84 +n_defs')

# PG Stuff
pg_connection = psycopg2.connect(database=db_name, user=db_user, password=db_pass, host=db_host)
cursor = pg_connection.cursor()

# Create the schema
schema = {'geometry': 'Point',
          'properties': [('spot_id', 'int'),
                         ('timestamp', 'datetime'),
                         ('reporter', 'str'),
                         ('reporter_grid', 'str'),
                         ('snr', 'int'),
                         ('frequency', 'int'),
                         ('call_sign', 'str'),
                         ('grid', 'str'),
                         ('power', 'str'),
                         ('drift', 'int'),
                         ('distance', 'int'),
                         ('azimuth', 'int'),
                         ('band', 'str'),
                         ('version', 'str'),
                         ('code', 'str')]}


def get_grid_centroids(grid):
    """
    Gets Maidenhead grid square centroid coordinates from PostGIS database
    :param grid: string denoting Maidenhead grid square
    :return: Lat/Lon tuple
    """
    accuracy = None

    # Query polygon centroid
    if len(grid) == 6:
        accuracy = 6
        cursor.execute(f"SELECT ST_Centroid(geom) as geom FROM vectors.maidenhead_grid WHERE "
                       "subgrid = '{grid}'")

    elif len(grid) == 4:
        accuracy = 4
        cursor.execute(f"SELECT ST_Centroid(geom) as geom FROM vectors.maidenhead_grid WHERE "
                       "grid = '{grid}'")

    # Get the data out of the cursor
    data = {'geom': None,
            'accuracy': None}

    for geom in cursor:
        data = {'geom': geom,
                'accuracy': accuracy}

    return data


def join_qso_to_grid(wspr_record):
    """
    Takes a row from the WSPR data as input and finds its grid. Then, the centroid for that grid is added to the
    WSPR record.
    :param qso_record: Line from WSPR data
    :return: WSPR data as a shapely object
    """

    wspr_tx_grid = wspr_record["grid"].lower()
    wspr_rx_grid = wspr_record["reporter_grid"].lower()

    tx_grid = get_grid_centroids(wspr_tx_grid)
    tx_grid_point = tx_grid["geom"]
    tx_grid_accuracy = tx_grid["accuracy"]

    rx_grid = get_grid_centroids(wspr_rx_grid)
    rx_grid_point = rx_grid["geom"]
    rx_grid_accuracy = rx_grid["accuracy"]

    wspr_record["tx_point"] = tx_grid_point
    wspr_record["tx_point_accuracy"] = tx_grid_accuracy
    wspr_record["rx_point"] = rx_grid_point
    wspr_record["rx_point_accuracy"] = rx_grid_accuracy

    return wspr_record


def create_geopackage(coords, out_dir):
    # Open connection with gpkg file in a context manager and write features to it
    output_path = os.path.join(out_dir, "wspr_contacts.gpkg")

    with fiona.open(output_path, 'w',
                    layer='points',
                    driver='GPKG',
                    schema=schema,
                    crs=crs) as dst:
        for qso in coords:  # TODO: Finish this
            continue
            # Create feature
            # Write feature


def csvs_in_path(path):
    """
    Finds CSVs in given path and returns the paths as a list
    :param path: String denoting path to CSV files
    :return: List of CSV files in path
    """

    csv_files = []

    for root, _, filenames in os.walk(path):
        for file in filenames:
            if os.path.splitext(file)[1] == '.csv':
                csv_path = os.path.join(root, file)
                csv_files.append(csv_path)

    return csv_files


def csv_to_dicts(csv_paths):
    """
    Converts CSV file to list of dicts
    :param csv_path: Path to WSPR CSV data
    :return: List of dicts
    """
    n_records = 0
    file_records = []
    for file in csv_paths:
        print(f"Processing {file}")

        with open(file, 'r') as csv_file:
            reader = csv.reader(csv_file)

            for row in reader:
                wspr_record = {}

                wspr_record["spot_id"] = row[0]
                wspr_record["timestamp"] = row[1]
                wspr_record["reporter_call"] = row[2]
                wspr_record["reporter_grid"] = row[3]
                wspr_record["snr"] = row[4]
                wspr_record["frequency"] = row[5]
                wspr_record["call_sign"] = row[6]
                wspr_record["grid"] = row[7]
                wspr_record["power"] = row[8]
                wspr_record["drift"] = row[9]
                wspr_record["distance"] = row[10]
                wspr_record["azimuth"] = row[11]
                wspr_record["band"] = row[12]
                wspr_record["version"] = row[13]
                wspr_record["code"] = row[14]

                file_records.append(wspr_record)

    n_records += len(file_records)
    print(f"{n_records} records processed\n")

    return file_records


def station_list(records):
    """
    Generate a dictionary containing unqique stations and their coordinates
    :param records: list of WSPR QSO dictionaries
    :return: List of dictionaries containing Call, Lat/Lon
    """

    stations = []

    for record in records:
        new_station = True
        station = {}
        rx_station = record["reporter_call"]
        rx_point = record["rx_point"]
        rx_point_accuracy = record["rx_point_accuracy"]
        tx_station = record["call_sign"]
        tx_point = record["tx_point"]
        tx_point_accuracy = record["tx_point_accuracy"]

        for station in stations:
            if station == rx_station or station == tx_station:
                new_station = False

        if not new_station:
            for call_sign in (rx_station, tx_station):
                continue


    return stations


def main():

    n_records = 0
    file_records = 0
    # geo_output_dir = input("geopackage output directory: ")
    csv_path = input("CSV input directory: ")

    csv_files = csvs_in_path(csv_path)
    records = csv_to_dicts(csv_files)

    # Get points
    for i, record in enumerate(records):
        records[i] = join_qso_to_grid(record)
        print(records[i])

main()
