import json
import logging
import requests
from decimal import Decimal
import datetime

import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')


    url = "https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/Current_WildlandFire_Locations/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json" 
    response = requests.get(url).json()
    fires = response['features']

    firesList = []

    for fire in fires:
        
        if fire["attributes"]["DailyAcres"] and fire["attributes"]["DailyAcres"] > 100:
            OBJECTID = fire["attributes"]["OBJECTID"]
            DailyAcres = fire["attributes"]["DailyAcres"]
            EstimatedCostToDate = fire["attributes"]["EstimatedCostToDate"]
            FireBehaviorGeneral = fire["attributes"]["FireBehaviorGeneral"]
            FireBehaviorGeneral1 = fire["attributes"]["FireBehaviorGeneral1"]
            FireBehaviorGeneral2 = fire["attributes"]["FireBehaviorGeneral2"]
            FireBehaviorGeneral3 = fire["attributes"]["FireBehaviorGeneral3"]
            FireDiscoveryDateTime = datetime.datetime.fromtimestamp((fire["attributes"]["FireDiscoveryDateTime"])/1000).strftime('%Y-%m-%d %H:%M:%S')
            FireMgmtComplexity = fire["attributes"]["FireMgmtComplexity"]
            IncidentName = fire["attributes"]["IncidentName"]
            IncidentShortDescription = fire["attributes"]["IncidentShortDescription"]
            TotalIncidentPersonnel = fire["attributes"]["TotalIncidentPersonnel"]
            CreatedOnDateTime_dt = datetime.datetime.fromtimestamp((fire["attributes"]["CreatedOnDateTime_dt"])/1000).strftime('%Y-%m-%d %H:%M:%S')
            ModifiedOnDateTime_dt = datetime.datetime.fromtimestamp(fire["attributes"]["ModifiedOnDateTime_dt"]/1000).strftime('%Y-%m-%d %H:%M:%S')
            # x = Decimal(fire["geometry"]["x"])
            # y = Decimal(fire["geometry"]["y"])
            x = fire["geometry"]["x"]
            y = fire["geometry"]["y"]

            args_tuple = (OBJECTID, DailyAcres, EstimatedCostToDate, FireBehaviorGeneral, FireBehaviorGeneral1, FireBehaviorGeneral2, FireBehaviorGeneral3, 
                FireDiscoveryDateTime, FireMgmtComplexity, IncidentName, IncidentShortDescription, TotalIncidentPersonnel, CreatedOnDateTime_dt, ModifiedOnDateTime_dt, x, y)
            
            firesList.append(args_tuple)



    return func.HttpResponse(json.dumps(firesList.json()))
