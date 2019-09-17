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
        writer = xlsWriter("C:/Naturindeks/vann-nett-elver.xlsx")
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
        writer = xlsWriter("C:/Naturindeks/vann-nett-sjoer.xlsx")

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
    attribute_df.set_index("StationId", inplace=True)

    point_df = pd.read_excel("c:/Naturindeks/Plankton.xlsx", "StationPoint")

    for idx, pti_row in pti_df.iterrows():
        stationid = pti_row[2]
        attribute_row = attribute_df[stationid]
        kommune = ""
        vannforekomst = ""
        if not attribute_row == np.nan:
            kommune = "{:04.0f}".format(attribute_df.at[stationid, "Kommunenummer"])
            vannforekomst = attribute_df.at[stationid, "VannforekomstID"]
        point = point_df.loc[point_df["StationId"] == stationid].iloc[0]
        print(point["Latitude"], point["Longitude"], str(pti_row[6])[0:10], pti_row[10], kommune, vannforekomst)


def rewriteNIVA_ASPT():
    aspt_df = pd.read_excel("c:/Naturindeks/Bunndyr.xlsx", "BunndyrVariables")

    attribute_df = pd.read_excel("c:/Naturindeks/Bunndyr.xlsx", "StationAttribute")
    attribute_df.set_index("StationId", inplace=True)

    point_df = pd.read_excel("c:/Naturindeks/Bunndyr.xlsx", "StationPoint")

    for idx, aspt_row in aspt_df.iterrows():
        stationid = aspt_row[2]
        attribute_row = attribute_df[stationid]
        kommune = ""
        vannforekomst = ""
        if not attribute_row == np.nan:
            kommune = "{:04.0f}".format(attribute_df.at[stationid, "Kommunenummer"])
            vannforekomst = attribute_df.at[stationid, "VannforekomstID"]
        point = point_df.loc[point_df["StationId"] == stationid].iloc[0]
        print(point["Latitude"], point["Longitude"], str(aspt_row[5])[0:10], aspt_row[6], kommune, vannforekomst)

