# GeoWSPR
Python scripts to download, parse, and convert selected WSPR CSV data to a PostGIS (PostgreSQL) DB for further GIS analysis.

## How to run
1. Run `create_config.py` to create the configuration file used by these scripts.
2. Download the Maidenhead grid squares from [here](https://geo.wardrup.me/documents/1#more)
3. Run `downloader.py` to download the CSV files. This script will take some time. Ensure it is still running from time
to time as it could be rate-limited by the website.
4. Run `csv_to_pg.py` to upload the CSV files to PostGIS. This too will take a considerable amount of time.