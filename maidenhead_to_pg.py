from wspr_pg_database import Base, maidenhead_grid, maidenhead_subgrid
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import fiona
from shapely.geometry import shape
from geoalchemy2.shape import from_shape
import shapely.ops
import os
import pathlib
import configparser

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

engine = create_engine(f'postgresql://{db_user}:{db_pass}@{db_host}/{db_name}')
Base.metadata.bind = engine

DBSession = sessionmaker(bind=engine)
session = DBSession()

"""
Load Maidenhead grid square shapefiles into PostGIS database.
"""


def list_shapefiles(input_path):
    """
    Get list of shapefiles in input path.
    :param input_path: Path to search.
    :return: List of paths pointing to shapefiles.
    """

    shp_paths = []
    for root, directory_name, filenames in os.walk(input_path):
        for file in filenames:
            if os.path.splitext(file)[1].lower() == '.shp':
                full_path =  os.path.join(root, file)
                shp_paths.append(full_path)

    return shp_paths


def read_shapefile(shp_path):
    """
    Reads a shapefile located at shp_path.
    :param shp_path: Path to shapefile.
    :return: A list of Shapely geometries for polygons in shapefile.
    """
    polygon_list = []
    polygons = fiona.open(shp_path)

    for polygon in polygons:
        grid = pathlib.Path(shp_path).stem
        subgrid = polygon['properties']['GridSquare']

        shapely_shp = shape(polygon['geometry'])
        # Get attributes from file
        shapely_shp.grid = grid
        shapely_shp.subgrid = subgrid

        polygon_list.append(shapely_shp)

    return polygon_list


def dissolve_subgrids(grid_polygons):
    """
    Dissolves subgrids within a grid to get main grid.
    :param grid_polygons: List of polygons within grid.
    :return: Shapely geometry for larger main grid.
    """

    dissolved_grid = shapely.ops.unary_union(grid_polygons)
    dissolved_grid.grid = grid_polygons[0].grid

    return dissolved_grid


def write_grid_to_db(grid):
    """
    Writes grid  to PostgreSQL database.
    :param grid: Shapely geometry for grid.
    """

    print(f"Writing {grid.grid}")
    db_grid = maidenhead_grid(
        grid=grid.grid,
        geom=from_shape(grid, srid=4326)
    )

    session.add(db_grid)
    session.commit()


def write_subgrid_to_db(subgrid):
    """
    Writes subgrid to PostgreSQL database.
    :param subgrid: Shapely geometry for subgrid.
    """

    for grid in subgrid:
        print(f"Writing {grid.grid}{grid.subgrid}")
        db_subgrid = maidenhead_subgrid(
            grid=grid.grid,
            subgrid=grid.subgrid,
            geom=from_shape(grid, srid=4326)
        )

        session.add(db_subgrid)
    session.commit()


def main():
    """
    Run the script.
    """
    maidenhead_grid_root_path = input("Maidenhead grid root path: ")
    shape_paths = list_shapefiles(maidenhead_grid_root_path)

    for shp in shape_paths:
        subgrid = read_shapefile(shp)
        grid = dissolve_subgrids(subgrid)

        query_result = session.query(maidenhead_grid.id).filter_by(grid=grid.grid).first()

        if not query_result:
            write_grid_to_db(grid)
            write_subgrid_to_db(subgrid)
        else:
            print(query_result)
            print(f"{grid} already exists in DB. Skipping.")


if __name__ == '__main__':
    main()
