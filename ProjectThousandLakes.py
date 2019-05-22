__author__ = 'Roar Brenden'

import AquaMonitor as am
import pandas as pd
from pandas import ExcelWriter as xlsWriter
from pandas import ExcelFile as xlsReader
import datetime
import requests
from pyproj import Proj, transform


# Project with stations in 2018
projectInnsjo2018 = 12368

# Project with the original stations from 1995
projectInnsjo1995 = 3659

# Project with stations for 2019
projectInnsjo2019 = 12433

am.host = "https://test-aquamonitor.niva.no/"


def create_am_file():
    am.Query("project_id=" + str(projectInnsjo2019))\
        .makeArchive("excel", "am1000sjoer.xlsx")\
        .download("c:/temp/")


def create_excel_file():
    stationsFrame = pd.read_excel("c:/temp/am1000sjoer.xlsx", "Station")

    writer = xlsWriter("c:/temp/1000sjoer.xlsx")

    outFrame = pd.DataFrame({"Station Id": stationsFrame["StationId"],
                         "Station Code": stationsFrame["StationCode"],
                         "Station Name": stationsFrame["StationName"]
                         })
    outFrame.set_index("Station Id", inplace=True)

    attributeFrame = pd.read_excel("c:/temp/am1000sjoer.xlsx", "StationAttribute")
    attributeFrame.set_index("StationId", inplace=True)

    outFrame["River/Lake Name"] = None
    outFrame["Kommune Nr"] = None
    outFrame["Kommune"] = None
    outFrame["Fylke Nr"] = None
    outFrame["Fylke"] = None
    outFrame["NVE Vatn Nr"] = None
    outFrame["Lake Area"] = None

    for stId, attributeRow in attributeFrame.iterrows():
        outFrame.at[stId, "River/Lake Name"] = attributeRow["Innsjønavn"]
        outFrame.at[stId, "Kommune Nr"] = attributeRow["Kommunenummer"]
        outFrame.at[stId, "Kommune"] = attributeRow["Kommunenavn"]
        outFrame.at[stId, "Fylke Nr"] = attributeRow["Fylkenummer"]
        outFrame.at[stId, "Fylke"] = attributeRow["Fylke"]
        outFrame.at[stId, "NVE Vatn Nr"] = attributeRow["Innsjønummer"]
        outFrame.at[stId, "Lake Area"] = attributeRow["Areal"]

    outFrame["UTM North"] = None
    outFrame["UTM East"] = None
    outFrame["UTM Zone"] = None

    pointFrame = pd.read_excel("c:/temp/am1000sjoer.xlsx", "StationPoint")
    pointFrame.set_index("StationId", inplace=True)

    for stId, pointRow in pointFrame.iterrows():
        if pointRow["EPSG"] > 32600:
            outFrame.at[stId, "UTM North"] = pointRow["Y"]
            outFrame.at[stId, "UTM East"] = pointRow["X"]
            outFrame.at[stId, "UTM Zone"] = pointRow["EPSG"] - 32600

    outFrame["Altitude"] = None

    outFrame.to_excel(writer)
    writer.save()


def generate_maps():
    meta = am.Query("project_id = " + str(projectInnsjo2019)).map("Metadata")
    for m in meta:
        for st in m["_Stations"]:
            if st["_Project"]["_Id"] == projectInnsjo2019:
                code = st["_Code"]
                break
        lon = m["_Longitude"]
        lat = m["_Latitude"]
        if lon > 24:
            epsg = 32635
        elif lon > 18:
            epsg = 32634
        elif lon > 12:
            epsg = 32633
        else:
            epsg = 32632
        x, y = transform(Proj(init="epsg:4326"), Proj(init="epsg:" + str(epsg)), lon, lat)

        bbox = str(x-3000) + "," + str(y-3000) + "," + str(x+3000) + "," + str(y+3000)
        width = 500
        height = 500
        resp = requests.get("https://test-aquamonitor.niva.no/geoserver/wms?" +
                            "service=WMS&version=1.1.0&request=GetMap&" +
                            "layers=no.norgedigitalt:Kartdata,no.niva.aquamonitor:Innsjo_stations&" +
                            "styles=,&bbox=" + bbox + "&width=" + str(width) + "&height=" + str(height) +
                            "&srs=EPSG:" + str(epsg) + "&format=image%2Fpng",
                            stream=True,
                            auth=("aquamonitor", "trommer"))

        if resp.status_code == 200:
            with open("C:/temp/" + code + ".png", "wb") as f:
                for chunk in resp:
                    f.write(chunk)
        else:
            print("Id:" + str(m["_Id"]) + " error code:" + str(resp.status_code))


create_excel_file()
