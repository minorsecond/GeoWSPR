from geoalchemy2 import *
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
import configparser

config = configparser.ConfigParser()
config.read("config")

db_user = config['DB']['db_user']
db_pass = config['DB']['db_pass']
db_host = config['DB']['db_host']
db_name = config['DB']['db_name']

"""
Define PG database & tables
"""

Base = declarative_base()


class maidenhead_grid(Base):
    __tablename__ = 'maidenhead_grid'
    __table_args__ = {"schema": "wspr"}
    id = Column(Integer, primary_key=True)
    grid = Column(String(4), nullable=False)
    geom = Column(Geometry(geometry_type='POLYGON', srid=4326))


class maidenhead_subgrid(Base):
    __tablename__ = 'maidenhead_subgrid'
    __table_args__ = {"schema": "wspr"}
    id = Column(Integer, primary_key=True)
    grid = Column(String(4), nullable=False)
    subgrid = Column(String(6), nullable=False)
    geom = Column(Geometry(geometry_type='POLYGON', srid=4326))


class wsprContact(Base):
    __tablename__ = 'wspr_data'
    __table_args__ = {"schema": "wspr"}
    id = Column(Integer, primary_key=True, autoincrement=True)
    spot_id = Column(Integer, primary_key=True)
    timestamp = Column('timestamp', Integer, nullable=False)
    rx_call = Column('rx_call', String(15), nullable=False)
    tx_call = Column('tx_call', String(15), nullable=False)
    rx_grid = Column('rx_grid', String(6), nullable=False)
    tx_grid = Column('tx_grid', String(6), nullable=False)
    snr = Column('snr', Integer, nullable=False)
    frequency = Column('frequency', Float, nullable=False)
    power = Column('power', Float, nullable=False)
    drift = Column('drift', Integer, nullable=False)
    distance = Column('distance', Integer, nullable=False)
    azimuth = Column('azimuth', Integer, nullable=False)
    band = Column('band', Integer, nullable=False)
    version = Column('version', String, nullable=True)
    code = Column('code', String, nullable=True)


class operator(Base):
    __tablename__ = "operators"
    __table_args__ = {"schema": "wspr"}
    id = Column(Integer, primary_key=True)
    callsign = Column('callsign', String(15))
    grid = Column('grid', String(6), nullable=False)
    geog = Column('geom', Geometry(geometry_type='POINT', srid=4326), nullable=True)


engine = create_engine(f'postgresql://{db_user}:{db_pass}@{db_host}/{db_name}')
metadata = MetaData(schema="wspr")

# Create the tables
Base.metadata.create_all(engine)
