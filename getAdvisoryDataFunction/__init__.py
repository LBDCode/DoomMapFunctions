import logging
import geopandas as gpd
import os
from sqlalchemy import create_engine
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    host =  os.environ['host']
    dbname = os.environ['dbname']
    user = os.environ['user']
    password = os.environ['password']

    db_connection_url = "postgresql://{0}:{1}@{2}/{3}".format(user, password, host, dbname)

    con = create_engine(db_connection_url)

    for advisory in ['Prcp', 'Temp', 'Soils']:
        url = "https://www.wpc.ncep.noaa.gov/threats/final/" + advisory + "_D3-7.zip"
        table = advisory + "_advisories"

        gdf = gpd.read_file(url)

        gdf = gdf.dropna(subset=['geometry'])

        # Push the geodataframe to postgresql
        gdf.to_postgis(table, con, index=False, if_exists='replace')

    return func.HttpResponse("", status_code=200)
