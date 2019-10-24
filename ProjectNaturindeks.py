import AquaMonitor as am
import requests as req
import json
import pandas as pd
from pandas import ExcelWriter as xlsWriter
import numpy as np


am.host = "https://test-aquamonitor.niva.no/"

def downloadNIVA_PTI():
    # PTI -> plankton.parameter_id = 7
    am.Query(where="Plankton.parameter_id=7")\
       .makeArchive(fileformat="excel", filename="Plankton.xlsx")\
       .download(path="C:/Naturindeks/")

def downloadNIVA_PIT_AIP():
    # PIT -> begroing.parameter_id = 1
    # AIP -> begroing.parameter_id = 2

    am.Query(where="Begroing.parameter_id in (1,2)")\
        .makeArchive(fileformat="excel", filename="Begroing.xlsx")\
        .download(path="C:/Naturindeks/")

def downloadNIVA_ASPT():
    am.Query(where="Bunndyr.parameter_id = 1")\
        .makeArchive(fileformat="excel", filename="Bunndyr.xlsx")\
        .download(path="C:/Naturindeks/")


def downloadVannNett(report):
    if report == "elver":
        req_data = {
            "ReportID": 110,
            "Page": 1,
            "HitsPerPage": 100,
            "WhereColumns": [],
            "SelectColumns": [3553, 3554, 3555, 3556, 3557, 3558, 3560, 3562, 3564, 3566, 3568, 3570, 3572, 3574, 3576, 3577, 3578]
        }
        writer = xlsWriter("C:/Naturindeks/Vann-nett-elver.xlsx")
    else:
        req_data = {
                "ReportID": 109,
                "Page": 1,
                "HitsPerPage": 100,
                "WhereColumns": [],
                "SelectColumns": [
                    3524, 3525, 3526, 3527, 3528, 3529, 3531, 3533,
                    3535, 3537, 3539, 3541, 3543, 3545, 3547, 3549,
                    3550, 3551
                ]}
        writer = xlsWriter("C:/Naturindeks/Vann-nett-sjoer.xlsx")

    out_frame = pd.DataFrame(columns=["VannforekomstID", "Vannforekomstnavn", "Vanntype", "Økoregion"])
    out_frame.set_index("VannforekomstID", inplace=True)

    out_frame["Vannforekomstnavn"] = None
    out_frame["Vanntype"] = None
    out_frame["Økoregion"] = None


    while True:
        resp = req.post("https://vann-nett.no/portal-api/api/ExecuteQuery", json=req_data)
        resp_data = json.loads(resp.text)
        resp_vannf = resp_data["Result"]

        for vannf in resp_vannf:
            vannforekomstid = vannf["VannforekomstID"]
            out_frame.at[vannforekomstid, "Vannforekomstnavn"] = vannf["Vannforekomstnavn"]
            out_frame.at[vannforekomstid, "Vanntype"] = vannf["Vanntype"]
            out_frame.at[vannforekomstid, "Økoregion"] = vannf["Økoregion"]

        print("Page: " + str(resp_data["Page"]) + " av " + str(resp_data["PageCount"]))
        if resp_data["Page"] == resp_data["PageCount"]:
            break

        req_data["Page"] = resp_data["Page"] + 1

    out_frame.to_excel(writer)
    writer.save()

def rewriteNIVA_PTI():
    pti_df = pd.read_excel("c:/Naturindeks/Plankton.xlsx", "PlanktonParameter", header=1)

    attribute_df = pd.read_excel("c:/Naturindeks/Plankton.xlsx", "StationAttribute")
    
    point_df = pd.read_excel("c:/Naturindeks/Plankton.xlsx", "StationPoint")

    vannett_df = pd.read_excel("c:/Naturindeks/Vann-nett-sjoer.xlsx", "Sheet1")

    data_rows = []
    for idx, pti_row in pti_df.iterrows():
        stationid = pti_row[2]
        attribute_row = attribute_df.loc[attribute_df["StationId"] == stationid].iloc[0]
        kommune = ""
        vannforekomst = ""
        if not attribute_row.empty:
            kommune = "{:04.0f}".format(attribute_row["Kommunenummer"])
            vannforekomst = attribute_row["VannforekomstID"]

        okoregion = ""
        vanntype = ""
        if not vannforekomst == "":
            try:
               vannett_row = vannett_df.loc[vannett_df["VannforekomstID"] == vannforekomst].iloc[0]
               if not vannett_row.empty:
                  okoregion = vannett_row["Økoregion"]
                  vanntype = vannett_row["Vanntype"]
            except IndexError:
                print(vannforekomst)
        point = point_df.loc[point_df["StationId"] == stationid].iloc[0]
        data_rows.append({"Latitude": point["Latitude"],
                          "Longitude": point["Longitude"],
                          "Date": str(pti_row[6])[0:10],
                          "PTI": pti_row[10],
                          "Kommunenr": kommune,
                          "VannforekomstID": vannforekomst,
                          "Økoregion": okoregion,
                          "Vanntype": vanntype})

    out_df = pd.DataFrame(data_rows, columns=["Latitude", "Longitude", "Date", "PTI", "Kommunenr", "VannforekomstID", "Økoregion", "Vanntype"])
    writer = xlsWriter("C:/Naturindeks/Naturindeks_plankton_niva.xlsx")
    out_df.to_excel(writer)
    writer.save()


def rewriteNIVA_ASPT():
    aspt_df = pd.read_excel("c:/Naturindeks/Bunndyr.xlsx", "BunndyrVariables")
    attribute_df = pd.read_excel("c:/Naturindeks/Bunndyr.xlsx", "StationAttribute")
    point_df = pd.read_excel("c:/Naturindeks/Bunndyr.xlsx", "StationPoint")

    data_rows = []
    for idx, aspt_row in aspt_df.iterrows():
        stationid = aspt_row[2]
        attribute_row = attribute_df.loc[attribute_df["StationId"] == stationid].iloc[0]
        kommune = ""
        vannforekomst = ""
        if not attribute_row.empty:
            kommune = "{:04.0f}".format(attribute_row["Kommunenummer"])
            vannforekomst = attribute_row["VannforekomstID"]
        point_row = point_df.loc[point_df["StationId"] == stationid].iloc[0]
        data_rows.append({"Latitude": point_row["Latitude"],
                       "Longitude": point_row["Longitude"],
                       "Date": str(aspt_row[5])[0:10],
                       "ASPT": aspt_row[6],
                       "Kommunenr": kommune,
                       "VannforekomstID": vannforekomst})

    out_df = pd.DataFrame(data_rows, columns=["Latitude", "Longitude", "Date", "ASPT", "Kommunenr", "VannforekomstID"])
    writer = xlsWriter("C:/Naturindeks/Naturindeks_bunndyr.xlsx")
    out_df.to_excel(writer)
    writer.save()

def callVannmiljoLokalitet(code):
    url = "https://kart.miljodirektoratet.no/arcgis/rest/services/vannmiljo/MapServer/1/query"
    params = {
                "where": "WaterLocationCode='" + code + "'",
                "outFields": "SourceID",
                "returnGeometry": True,
                "outSR": 4326,
                "f": "pjson"
    }
    try:
       resp = req.post(url, params)
       features = json.loads(resp.text)["features"]
    except Exception as ex:
        print(ex)
        features = []

    if len(features) == 1:
        return features[0]
    else:
        return None


def callGeoserverQueryVannforekomstF(latitude, longitude):
    url = "http://www.aquamonitor.no/geoserver/rest/query/no.niva/nve_vannforekomst_f/distance/4326_" \
            + str(latitude) + "_" + str(longitude) + "_0.01/features.json"
    resp = req.get(url)
    features = json.loads(resp.text)["features"]
    if len(features) == 1:
        return features[0]["vannforekomstid"]
    else:
        return None

def callGeoserverQueryKommuneF(latitude, longitude):
    url = "http://www.aquamonitor.no/geoserver/rest/query/no.niva/n250_komm_f/distance/4326_" \
          + str(latitude) + "_" + str(longitude) + "_0.01/features.json"
    resp = req.get(url)
    features = json.loads(resp.text)["features"]
    if len(features) == 1:
        return features[0]["komm"]
    else:
        return None

def issueVannmiljoPPTIDownloadfile():
    url = "https://vannmiljo.miljodirektoratet.no/ExportRegistrations"
    params = {
        "ParametersIDs": ["PPTI"],
        "ExportEmail": "roar.branden@niva.no",
        "ExportType": "redigering",
        "RegType": 1
    }
    req.post(url, params)

def rewriteVannmiljo_PTI():
    vannmiljo_df = pd.read_excel("C:/Naturindeks/ppti/VannmiljoEksport_vannreg.xlsx", "VannmiljoEksport")
    vannett_df = pd.read_excel("c:/Naturindeks/Vann-nett-sjoer.xlsx", "Sheet1")
    data_rows = []
    for idx,vannmiljo_row in vannmiljo_df.iterrows():
        vannlok = callVannmiljoLokalitet(vannmiljo_row["Vannlok_kode"])
        if vannlok is not None:
            latitude = vannlok["geometry"]["y"]
            longitude = vannlok["geometry"]["x"]
            vannforekomst = callGeoserverQueryVannforekomstF(latitude, longitude)
            kommune = callGeoserverQueryKommuneF(latitude, longitude)
            okoregion = ""
            vanntype = ""
            if vannforekomst is not None:
                try:
                   vannett_row = vannett_df.loc[vannett_df["VannforekomstID"] == vannforekomst].iloc[0]
                   if not vannett_row.empty:
                      okoregion = vannett_row["Økoregion"]
                      vanntype = vannett_row["Vanntype"]
                except IndexError:
                    print(vannforekomst)

            stationId = ""
            sourceId = str(vannlok["attributes"]["SourceID"])
            if len(sourceId) > 5 and sourceId[:5] == "NIVA@":
                stationId = sourceId[5:]

            planktonId = ""
            lokalId = str(vannmiljo_row["ID_lokal"])
            if len(lokalId) > 9 and lokalId[:9] == "NIVA@PLA@":
                planktonId = lokalId[9:]

            date = vannmiljo_row["Tid_provetak"][8:10] + "." + \
                   vannmiljo_row["Tid_provetak"][5:7] + "." + \
                   vannmiljo_row["Tid_provetak"][0:4]

            data_rows.append({
                "Latitude": latitude,
                "Longitude": longitude,
                "Date": date,
                "PTI": float(vannmiljo_row["Verdi"].replace(",", ".")),
                "Kommune": kommune,
                "VannforekomstID": vannforekomst,
                "Økoregion": okoregion,
                "Vanntype": vanntype,
                "Plankton_parameter_values_id": planktonId,
                "Station_id": stationId
            })

    out_df = pd.DataFrame(data_rows, columns=["Latitude", "Longitude", "Date", "PTI",
                                              "Kommune", "VannforekomstID", "Økoregion", "Vanntype",
                                              "Plankton_parameter_values_id", "Station_id"])
    writer = xlsWriter("C:/Naturindeks/Naturindeks_plankton_vannmiljo.xlsx")
    out_df.to_excel(writer)
    writer.save()



