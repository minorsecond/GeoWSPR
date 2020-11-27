import os
from time import sleep

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import conf_reader
from wspr_pg_database import wsprContact, Base, operator, maidenhead_grid, \
    maidenhead_subgrid

"""
Upload CSV files in directory to PG database
"""

db_user, db_pass, db_host, db_name = conf_reader.read_config()

engine = create_engine(f'postgresql://{db_user}:{db_pass}@{db_host}/{db_name}')
Base.metadata.bind = engine

DBSession = sessionmaker(bind=engine)
session = DBSession()

# Get list of CSV files
# Iterate through CSV files and upload PG db. Notify user every 1000 rows.

header = ('spot_id', 'timestamp', 'reporter', 'reporters_grid',
          'snr', 'frequency', 'call_sign', 'grid', 'power', 'drift',
          'distance', 'azimuth', 'band', 'version', 'code')

processed_qsos = []


def add_ops(ops):
    for i in range(0, 1):
        if i == 0:
            op = str(ops["rx_op"]).lower()
            grid = str(ops["rx_grid"]).lower()
            geom = ops["rx_geog"]
        else:
            op = str(ops["tx_op"]).lower()
            grid = str(ops["tx_grid"]).lower()
            geom = ops["tx_geog"]

        base_grid = grid[:4]

        # Check if operator already exists in DB
        pg_op = session.query(operator). \
            filter(operator.callsign == op,
                   operator.grid.like(f"{base_grid}%")).first()

        if not pg_op:
            new_operator = operator(callsign=op, grid=grid,
                                    geog=get_operator_coords(grid))
            session.add(new_operator)

        else:  # Replace grid with subgrid if possible, as it's more precise
            if len(pg_op.grid) < len(grid):
                print(f"Updating grid for {op} with a more precise subgrid")
                pg_op.grid = grid
                pg_op.geom = get_operator_coords(grid)

    session.commit()


def get_operator_coords(grid):
    grid = grid.lower()

    if len(grid) == 6:
        try:
            geog = session.query(maidenhead_subgrid.geom).filter(
                maidenhead_subgrid.subgrid == grid).first().geom.ST_Centroid()
        except AttributeError:
            geog = None

    elif len(grid) == 4:
        try:
            geog = session.query(maidenhead_grid.geom).filter(maidenhead_grid.grid == grid).first().geom.ST_Centroid()
        except AttributeError:
            geog = None

    else:
        geog = None

    return geog


def process(csv_chunk):
    qsos = csv_chunk.values.tolist()

    for qso in qsos:
        spot_id = qso[0]
        timestamp = qso[1]
        reporter = qso[2]
        reporters_grid = qso[3]
        snr = qso[4]
        frequency = qso[5]
        call_sign = qso[6]
        grid = qso[7]
        power = qso[8]
        drift = qso[9]
        distance = qso[10]
        azimuth = qso[11]
        band = qso[12]
        version = qso[13]
        code = qso[14]
        rx_geog = None
        tx_geog = None

        operators = {'rx_op': reporter,
                     'rx_grid': reporters_grid,
                     'tx_op': call_sign,
                     'tx_grid': grid,
                     'rx_geog': rx_geog,
                     'tx_geog': tx_geog}

        add_ops(operators)  # upload the operators

        if spot_id not in processed_qsos:
            new_qso = wsprContact(spot_id=spot_id,
                                  timestamp=timestamp,
                                  rx_call=reporter,
                                  rx_grid=reporters_grid,
                                  tx_call=call_sign,
                                  tx_grid=grid,
                                  snr=snr,
                                  frequency=frequency,
                                  power=power,
                                  drift=drift,
                                  distance=distance,
                                  azimuth=azimuth,
                                  band=band,
                                  version=version,
                                  code=code)

            session.add(new_qso)
            processed_qsos.append(spot_id)
    session.commit()


def csv_chunk(csv_path, n_rows, processed_rows, processed_files):
    chunksize = 1000
    current_file_rows = 0
    for chunk in pd.read_csv(csv_path, chunksize=chunksize, header=None):
        process(chunk)
        processed_rows += chunksize
        current_file_rows += chunksize
        print(f"Processed {current_file_rows} rows out of {n_rows} from {csv_path}. {processed_rows} total rows from \
        {processed_files} files")

    return processed_rows


def csv_processor(root_path):
    processed_rows = 0
    processed_files = 1
    for root, _, filenames in os.walk(root_path):
        for file in filenames:
            if os.path.splitext(file)[1] == '.csv':
                csv_path = os.path.join(root, file)
                print(f"Processing {csv_path}")
                sleep(3)

                print("Counting all rows...")
                n_rows = sum(1 for row in open(csv_path))

                print("Now processing...")
                processed_rows += csv_chunk(csv_path, n_rows, processed_rows, processed_files)
                processed_files += 1


def main():
    root_path = input("WSPR CSV root path: ")
    csv_processor(root_path)


if __name__ == '__main__':
    main()
