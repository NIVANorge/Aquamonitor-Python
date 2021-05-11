__author__ = "Roar Brenden"

import configparser
import datetime
import getpass
import json
import os
import pyexpat
import time
from xml.dom import minidom

import numpy as np
import pandas as pd
import requests
from pandas import json_normalize

host = "https://aquamonitor.niva.no/"
aqua_site = "AquaServices"
archive_site = "AquaServices"
cache_site = "AquaCache"


def requestService(url, params):
    response = requests.post(url, params)
    try:
        return minidom.parseString(response.text)
    except pyexpat.ExpatError as e:
        print("URL: " + url)
        print(
            "PARAMS:"
            + "".join("{}={} ".format(key, val) for key, val in params.items())
        )
        print("RESPONSE:" + response.text)
        raise e


def login(username=None, password=None):
    """Login to Aquamonitor with your username and password. For security, avoid passing the args
        directly. The function will first attempt to read stored credentials from a '.auth' file
        and, if this is not possible, it will prompt for your username and password.

    Args:
        username: Str. Optional. Aquamonitor username
        password: Str. Optional. Aquamonitor password

    Returns:
        Str. Access token valid for one day.
    """
    if username is None:
        authfile = os.path.join(os.path.dirname(__file__), ".auth")
        if os.path.isfile(authfile):
            config = configparser.RawConfigParser()
            try:
                config.read(authfile)
                username = config.get("Auth", "username")
                password = config.get("Auth", "password")
            except Exception as ex:
                raise Exception("Couldn't read username/password from .auth file.")
        else:
            print("Please enter your credentials.")
            username = getpass.getpass(prompt="Username: ")
            password = getpass.getpass(prompt="Password: ")

    loginurl = aqua_site + "/login"
    loginparams = {"username": username, "password": password}
    userdict = postJson(None, loginurl, loginparams)
    usertype = userdict["Usertype"]

    if not usertype == "NoUser":
        token = userdict["Token"]
    else:
        raise Exception("Login failed. Please check your username and password.")

    return token


def get(token: str, site: str, path: str, stream: bool = False):
    return requests.get(host + site + path, cookies=dict(aqua_key=token), stream=stream)


def reportJsonError(url, response):
    message = (
        "AquaMonitor failed with status: " + str(response.status_code) + " for url: " + url + "\nMessage: "
    )
    if response.text is not None:
        try:
            message = message + json.loads(response.text).get("Message")
        except:
            message = message + "No JSON in response."

    raise Exception(message)


def getJson(token, path):
    response = requests.get(host + path, cookies=dict(aqua_key=token))
    if response.status_code == 200:
        return json.loads(response.text)
    else:
        reportJsonError(host + path, response)


def postJson(token, path, inJson):
    response = requests.post(host + path, json=inJson, cookies=dict(aqua_key=token))
    if response.status_code == 200:
        return json.loads(response.text)
    else:
        reportJsonError(host + path, response)


def putJson(token, path, inJson):
    response = requests.put(host + path, json=inJson, cookies=dict(aqua_key=token))
    return json.loads(response.text)


def deleteJson(token, path):
    response = requests.delete(host + path, cookies=dict(aqua_key=token))
    return json.loads(response.text)


def getProject(token, projectId):
    projectsurl = "AquaServices/api/projects/" + str(projectId)
    return getJson(token, projectsurl)


def getStations(token, projectId):
    stationsurl = "AquaServices/api/projects/" + str(projectId) + "/stations/"
    return getJson(token, stationsurl)


def getArchive(token, id):
    path = archive_site + "/files/archive/" + id
    return getJson(token, path)


def createDatafile(token, data):
    path = archive_site + "/files/datafile/"
    return postJson(token, path, data)


def deleteArchive(token, id):
    path = archive_site + "/files/archive/" + id
    return deleteJson(token, path)


def downloadFile(token: str, url: str, path: str) -> None:
    resp = get(token, "", url)
    with open(path, "wb") as fd:
        for chunk in resp.iter_content(chunk_size=256):
            fd.write(chunk)


def downloadArchive(token, id, file, path):
    downloadFile(token, archive_site + "/files/archive/" + id + "/" + file, path)


class Query:
    token = None
    key = None
    table = None
    result = None

    def __init__(self, where=None, token=None):
        self.where = where
        self.token = token

    def map(self, table=None):
        if self.token is None:
            self.token = login()

        self.table = table

        if self.key is None:
            self.createQuery()

        if self.key is not None:
            self.waitQuery()
            if self.result.get("ErrorMessage") is None:
                if table is None:
                    return self.result["CurrentStationIds"]
                else:
                    return Pages(self, self.result)
            else:
                raise Exception(
                    "Query ended with an error: " + self.result["ErrorMessage"]
                )

    def makeArchive(self, fileformat, filename):
        if self.token is None:
            self.token = login()

        if self.key is None:
            self.createQuery()

        if not self.key is None:
            self.waitQuery()
            if self.result.get("ErrorMessage") is None:
                return Archive(
                    fileformat,
                    filename,
                    token=self.token,
                    stations=self.result["CurrentStationIds"],
                    where=self.where,
                )
            else:
                raise Exception(
                    "Query ended with an error: " + self.result["ErrorMessage"]
                )

    def waitQuery(self):
        resp = getJson(self.token, cache_site + "/query/" + self.key)

        if not resp.get("Result") is None:
            while not resp["Result"]["Ready"]:
                time.sleep(1)
                resp = getJson(self.token, cache_site + "/query/" + self.key)
            if self.table is None:
                self.result = resp["Result"]
            else:
                resp = getJson(
                    self.token, cache_site + "/query/" + self.key + "/" + self.table
                )

                if not resp.get("Ready") is None:
                    while not resp["Ready"]:
                        time.sleep(1)
                        resp = getJson(
                            self.token,
                            cache_site + "/query/" + self.key + "/" + self.table,
                        )
                    self.result = resp
                else:
                    raise Exception(
                        "Query didn't respond properly for table request: " + self.table
                    )
        else:
            raise Exception("Query didn't respond properly.")

    def createQuery(self):
        query = {}

        if self.table is not None:
            query["From"] = [{"Table": self.table}]

        if self.where is not None:
            query["Where"] = self.where

        resp = postJson(self.token, cache_site + "/query/", query)

        if resp.get("Key") is None:
            raise Exception("Couldn't create query. Response: " + str(resp))
        else:
            self.key = resp["Key"]


class Pages:
    token = None
    key = None
    table = None
    total = 0
    pages = 0

    def __init__(self, query, result):
        self.token = query.token
        self.key = query.key
        self.table = query.table
        self.total = result["Total"]
        self.pages = result["Pages"]

    def fetch(self, page):
        if self.pages > page >= 0:
            resp = getJson(
                self.token,
                cache_site + "/query/" + self.key + "/" + self.table + "/" + str(page),
            )
            if not resp.get("Items") is None:
                return resp.get("Items")
            else:
                raise Exception("Page wasn't ready.")
        else:
            raise Exception("Page outside of range.")


class Archive:
    id = None
    expires = None
    token = None

    def __init__(self, *args, **kwargs):
        if args.__len__() == 1:
            self.id = args[0]
        elif args.__len__() == 2:
            self.fileformat = args[0]
            self.filename = args[1]

        self.token = kwargs.get("token")
        self.stations = kwargs.get("stations")
        self.where = kwargs.get("where")

    def download(self, path):
        if self.id is None:
            self.createArchive()

        if not self.id is None:
            resp = getArchive(self.token, self.id)
            while resp.get("Archived") is None:
                time.sleep(5)
                resp = getArchive(self.token, self.id)

            for file in resp["Files"]:
                downloadArchive(
                    self.token, self.id, file["FileName"], path + file["FileName"]
                )
        else:
            print("Couldn't create archive.")

    def createArchive(self):
        if self.expires is None:
            self.expires = datetime.date.today() + datetime.timedelta(days=1)

        if self.token is None:
            self.token = login()

        if self.fileformat == "excel":
            content_type = "application/vnd.ms-excel"
        elif self.fileformat == "csv":
            content_type = "text/csv"
        else:
            content_type = "text/plain"

        archive = {
            "Expires": self.expires.strftime("%Y.%m.%d"),
            "Title": "QueryExample",
            "Files": [{"Filename": self.filename, "ContentType": content_type}],
            "Definition": {
                "Format": self.fileformat,
                "StationIds": self.stations,
                "DataWhere": self.where,
            },
        }
        resp = createDatafile(self.token, archive)
        if not resp.get("Id") is None:
            self.id = resp["Id"]


class Graph:
    token = None
    site = None
    url = None

    def __init__(self, width: int, height: int, **kwargs):
        self.token = kwargs.get("token")
        self.site = kwargs.get("site")

        self.url = (
            kwargs.get("graph")
            + "?w="
            + str(width)
            + "&h="
            + str(height)
            + "&stid="
            + str(kwargs.get("stationId"))
            + "&p="
            + kwargs.get("parameter")
            + "&where="
            + kwargs.get("where")
        )

    def download(self, path: str):
        response = get(self.token, self.site, self.url, stream=True)
        if response.status_code == 200:
            with open(path, "wb") as file:
                for chunk in response.iter_content():
                    file.write(chunk)


def get_project_chemistry(proj_id, st_dt, end_dt, token=None):
    """Get all water chemistry data for the specified project ID and date range.

    Args:
        proj_id:  Int.
        st_dt:    Str. Start of period of interest in format 'dd.mm.yyyy'
        end_dt:   Str. End of period of interest in format 'dd.mm.yyyy'
        token:    Str. Optional. Valid API access token. If None, will first attempt to read
                  credentials from a '.auth' file in the installation folder. If this fails,
                  will prompt for username and password

    Returns:
        Dataframe.
    """
    # Query API and save result-set to cache
    where = (
        f"project_id = {proj_id} and sample_date >= {st_dt} and sample_date <= {end_dt}"
    )
    table = "water_chemistry_output"
    query = Query(where=where, token=token)
    pages = query.map(table)

    # Iterate over cache and build dataframe
    df_list = []
    for page in range(pages.pages):
        resp = pages.fetch(page)
        df_list.append(json_normalize(resp))

    df = pd.concat(df_list, axis="rows")

    df.dropna(subset=["Value"], inplace=True)

    # Tidy
    df.drop(
        ["$type", "Sample.$type", "Parameter.Id", "Sample.Id"],
        axis="columns",
        inplace=True,
    )

    df["Sample.SampleDate"] = pd.to_datetime(df["Sample.SampleDate"])

    # if "Sample.Depth1" not in df.columns:
    #     df["Sample.Depth1"] = np.nan

    # if "Sample.Depth2" not in df.columns:
    #     df["Sample.Depth2"] = np.nan

    df.rename(
        {
            "Sample.SampleDate": "SampleDate",
            "Sample.Station.Id": "StationId",
            "Sample.Station.Code": "StationCode",
            "Sample.Station.Name": "StationName",
            "Sample.Station.Project.Id": "ProjectId",
            "Sample.Station.Project.Name": "ProjectName",
            "Parameter.Name": "ParameterName",
            "Parameter.Unit": "Unit",
            "Sample.Depth1": "Depth1",
            "Sample.Depth2": "Depth2",
        },
        axis="columns",
        inplace=True,
    )

    df = df[
        [
            "ProjectId",
            "ProjectName",
            "StationId",
            "StationCode",
            "StationName",
            "SampleDate",
            "Depth1",
            "Depth2",
            "ParameterName",
            "Flag",
            "Value",
            "Unit",
        ]
    ]

    df.sort_values(
        ["ProjectId", "StationId", "SampleDate", "Depth1", "Depth2", "ParameterName"],
        inplace=True,
    )
    df.reset_index(inplace=True, drop=True)

    return df


def get_projects(token=None):
    """Get full list of projects in the Nivadatabase/Aquamonitor.

    Args:
        token: Str. Optional. Valid API access token. If None, will first attempt to read
               credentials from a '.auth' file in the installation folder. If this fails,
               will prompt for username and password

    Returns:
        Dataframe
    """
    if not token:
        token = login()

    resp = getJson(token, aqua_site + "/api/Projects")
    df = json_normalize(resp)

    # Tidy
    df.rename(
        {
            "Id": "ProjectId",
            "Name": "ProjectName",
            "Description": "Description",
            "Number": "ProjectCode",
        },
        inplace=True,
        axis="columns",
    )
    df = df[
        [
            "ProjectId",
            "ProjectCode",
            "ProjectName",
            "Description",
        ]
    ]

    df.sort_values(["ProjectId"], inplace=True)
    df.reset_index(inplace=True, drop=True)

    return df


def get_project_stations(proj_id, token=None):
    """Get stations associated with a specific project.

    Args:
        proj_id: Int. Project ID for project of interest
        token:   Str. Optional. Valid API access token. If None, will first attempt to read
                 credentials from a '.auth' file in the installation folder. If this fails,
                 will prompt for username and password

    Returns:
        Dataframe
    """
    if not token:
        token = login()

    resp = getJson(token, aqua_site + f"/api/projects/{proj_id}/stations")
    df = json_normalize(resp)

    # Tidy
    del df["Type._Id"]
    df.rename(
        {
            "Id": "StationId",
            "Project.Id": "ProjectId",
            "Code": "StationCode",
            "Name": "StationName",
            "Type._Text": "Type",
        },
        inplace=True,
        axis="columns",
    )
    df = df[["ProjectId", "StationId", "StationCode", "StationName", "Type"]]
    df.sort_values(["ProjectId", "StationId"], inplace=True)
    df.reset_index(inplace=True, drop=True)

    return df
