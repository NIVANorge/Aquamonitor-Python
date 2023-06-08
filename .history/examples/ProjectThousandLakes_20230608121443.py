__author__ = 'Roar Brenden'

import aquamonitor.aquamonitor as am
import pandas as pd
from pandas import ExcelWriter as xlsWriter
import requests
import json
from pyproj import Proj, transform


# Project with stations in 2018
projectInnsjo2018 = 12368

# Project with the original stations from 1995
projectInnsjo1995 = 3659

# Project with stations for 2019
projectInnsjo2019 = 12433

#am.host = "https://test-aquamonitor.niva.no/"

geoserverUrl = "https://aquamonitor.niva.no/geoserver"

def download_am_file():
    am.Query("project_id=" + str(projectInnsjo2019))\
        .makeArchive("excel", "am1000sjoer.xlsx")\
        .download("c:/Innsjo2019/")


def create_excel_file(outFile):
    stationsFrame = pd.read_excel("c:/Innsjo2019/am1000sjoer.xlsx", "Stasjoner")

    writer = xlsWriter("c:/Innsjo2019/" + outFile)

    outFrame = pd.DataFrame({"Station Id": stationsFrame["StationId"],
                         "Station Code": stationsFrame["StationCode"],
                         "Station Name": stationsFrame["StationName"]
                         })
    outFrame.set_index("Station Id", inplace=True)
    outFrame["River/Lake Name"] = None
    outFrame["Kommune Nr"] = None
    outFrame["Kommune"] = None
    outFrame["Fylke Nr"] = None
    outFrame["Fylke"] = None
    outFrame["Lake Nr"] = None
    outFrame["Lake Area"] = None
    outFrame["Naturvern"] = None
    outFrame["Verneform"] = None
    outFrame["AquaMonitor"] = None

    attributeFrame = pd.read_excel("c:/Innsjo2019/am1000sjoer.xlsx", "StationAttribute")
    attributeFrame.set_index("StationId", inplace=True)


    for stId, attributeRow in attributeFrame.iterrows():
        outFrame.at[stId, "River/Lake Name"] = attributeRow["Innsjønavn"]
        outFrame.at[stId, "Kommune Nr"] = attributeRow["Kommunenummer"]
        outFrame.at[stId, "Kommune"] = attributeRow["Kommunenavn"]
        outFrame.at[stId, "Fylke Nr"] = attributeRow["Fylkenummer"]
        outFrame.at[stId, "Fylke"] = attributeRow["Fylke"]
        outFrame.at[stId, "Lake Nr"] = attributeRow["Innsjønummer"]
        outFrame.at[stId, "Lake Area"] = attributeRow["Areal"]
        outFrame.at[stId, "Naturvern"] = attributeRow["Naturvern"]
        outFrame.at[stId, "Verneform"] = attributeRow["Naturvernform"]

    outFrame["UTM North"] = None
    outFrame["UTM East"] = None
    outFrame["UTM Zone"] = None

    pointFrame = pd.read_excel("c:/Innsjo2019/am1000sjoer.xlsx", "Stasjoner")
    pointFrame.set_index("StationId", inplace=True)

    for stId, pointRow in pointFrame.iterrows():
        if pointRow["EPSG"] > 32600:
            outFrame.at[stId, "UTM North"] = pointRow["Y"]
            outFrame.at[stId, "UTM East"] = pointRow["X"]
            outFrame.at[stId, "UTM Zone"] = pointRow["EPSG"] - 32600
        else:
            lon = pointRow["Longitude"]
            lat = pointRow["Latitude"]
            if lon > 24:
                epsg = 32635
            elif lon > 18:
                epsg = 32634
            elif lon > 12:
                epsg = 32633
            else:
                epsg = 32632
            x, y = transform(Proj(init="epsg:4326"), Proj(init="epsg:" + str(epsg)), lon, lat)
            outFrame.at[stId, "UTM North"] = y
            outFrame.at[stId, "UTM East"] = x
            outFrame.at[stId, "UTM Zone"] = epsg - 32600

    outFrame["Altitude"] = None

    outFrame.to_excel(writer)
    writer.save()


def generate_maps():
    meta = am.Query("project_id = " + str(projectInnsjo2019), table="Metadata").list()
    for m in meta:
        sid = m["_Id"]
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
        width = 300
        height = 300
        resp = requests.get(geoserverUrl + "/wms?" +
                            "service=WMS&version=1.1.0&request=GetMap&" +
                            "layers=no.norgedigitalt:kartdata,no.niva.aquamonitor:Innsjo_stations&" +
                            "styles=,&bbox=" + bbox + "&width=" + str(width) + "&height=" + str(height) +
                            "&srs=EPSG:" + str(epsg) + "&format=image%2Fpng",
                            stream=True,
                            auth=("aquamonitor", "trommer"))

        if resp.status_code == 200:
            with open("C:/Innsjo2019/kart/" + str(sid) + ".png", "wb") as f:
                for chunk in resp:
                    f.write(chunk)
        else:
            print("Id:" + str(sid) + " error code:" + str(resp.status_code))


def generate_map(stationId):
    result = am.Query("station_id = " + str(stationId), table="Metadata").list()
    print(result.pages)

    for i in range(result.pages):
        meta = result.fetch(i)
        for m in meta:
            sid = m["_Id"]
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
            width = 300
            height = 300
            resp = requests.get(geoserverUrl + "/wms?" +
                                "service=WMS&version=1.1.0&request=GetMap&" +
                                "layers=no.norgedigitalt:kartdata,no.niva.aquamonitor:Innsjo_stations&" +
                                "styles=,&bbox=" + bbox + "&width=" + str(width) + "&height=" + str(height) +
                                "&srs=EPSG:" + str(epsg) + "&format=image%2Fpng",
                                stream=True,
                                auth=("aquamonitor", "trommer"))

            if resp.status_code == 200:
                with open("C:/Innsjo2019/kart/" + str(sid) + ".png", "wb") as f:
                    for chunk in resp:
                        f.write(chunk)
            else:
                print("Id:" + str(sid) + " error code:" + str(resp.status_code))


#download_am_file()
#create_excel_file("1000innsjoer_2.xlsx")
generate_maps()
