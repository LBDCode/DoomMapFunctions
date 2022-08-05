import logging
import geopandas as gpd
import os
import requests
from sqlalchemy import create_engine
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    host =  os.environ['host']
    dbname = os.environ['dbname']
    user = os.environ['user']
    password = os.environ['password']

    db_connection_url = "postgresql://{0}:{1}@{2}/{3}".format(user, password, host, dbname)

    # db_connection_url = "postgresql://doommap2022:blueridge9!23@doommapserver.postgres.database.azure.com/DoomMap"
    con = create_engine(db_connection_url)

    url = 'https://droughtmonitor.unl.edu/data/shapefiles_m/USDM_current_M.zip'  

    gdf = gpd.read_file(url)

    gdf = gdf.dropna(subset=['geometry'])

    gdf.to_postgis("drought_conditions", con, index=False, if_exists='replace') 

    return func.HttpResponse("", status_code=200)
