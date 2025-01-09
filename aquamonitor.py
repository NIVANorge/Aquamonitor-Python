__author__ = "Roar Brenden"

import datetime
import getpass
import json
import os
import time
from xml.dom import minidom
import pandas as pd
import pyexpat
import requests
from joblib import Parallel, delayed
from pandas import json_normalize
from yaspin import yaspin
from yaspin.spinners import Spinners

host = "https://aquamonitor.niva.no/"
aqua_site = "AquaServices"
api_site = "AquaServices"
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
       directly. The function will first attempt to read stored credentials from two environment
       variables: AQUAMONITOR_USER and AQUAMONITOR_PASSWORD. If this is not possible, it will
       prompt for your username and password.

    Args:
        username: Str. Optional. Aquamonitor username
        password: Str. Optional. Aquamonitor password

    Returns:
        Str. Access token valid for one day.
    """
    if username is None:
        try:
            username = os.environ["AQUAMONITOR_USER"]
            password = os.environ["AQUAMONITOR_PASSWORD"]
        except KeyError:
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
        "AquaMonitor failed with status: "
        + str(response.status_code)
        + " for url: "
        + url
        + "\nMessage: "
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
    projectsurl = api_site + "/api/projects/" + str(projectId)
    return getJson(token, projectsurl)


def getStations(token, projectId):
    stationsurl = api_site + "/api/projects/" + str(projectId) + "/stations/"
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

def geoQuery(layer: str, token: str = None, filter: str = None) :
    """ Queries Geoserver for stations. The arguments specifies a layer within 
        Geoserver that should be used to get the area to query within. And the filter
        is to limit it further.

        The layer should be in namespace: no.niva
        The stations are fetched from layer: no.niva.aquamonitor:Intern_stations

        The returned value is a json with the fields:
        STATION_TYPE_ID,FULL_STATION_NAME,LONGITUDE,SAMPLE_POINT_ID,STATION_CODE,
        LATITUDE,STATION_ID,PROJECT_ID,PROJECT_NAME,STATION_TYPE,STATION_NAME 
    """
    if token is None:
        token = login()

    if filter is None:
        filter = "0=0"
    
    ret = getJson(token, aqua_site + "/Gis/Query.ashx?q=no.niva.aquamonitor/Intern_stations/filter/no.niva/" + layer + "/" + filter + "/features.json")
    return ret.get("features")

class Query:
    token = None
    result = None
    selectedStations = None

    def __init__(self, where=None, token=None, stations=None, key=None, table=None):
        self.where = where
        self.token = token
        self.selectedStations = stations
        self.key = key
        self.table = table

    def createQuery(self):
        if self.token is None:
            self.token = login()
        query = {}
        if self.table is not None:
            query["From"] = [{"Table": self.table}]
        if self.where is not None:
            query["Where"] = self.where
        if self.selectedStations is not None:
            query["SelectedStations"] = self.selectedStations
        resp = postJson(self.token, cache_site + "/query/", query)
        if resp.get("Key") is None:
            raise Exception("Couldn't create query. Response: " + str(resp))
        else:
            self.key = resp["Key"]

    def readQuery(self):
        resp = getJson(self.token, cache_site + "/query/" + self.key)
        self.table = resp["Query"]["From"][0]["Table"]
        self.selectedStations = resp["Query"]["SelectedStations"]
        self.where = resp["Query"]["Where"]
        self.result = resp["Result"]

    def list(self):
        if self.token is None:
            self.token = login()
        if self.key is None:
            self.createQuery()
        else:
            self.readQuery()
        if self.key is not None:
            self.waitQuery()
            if self.result.get("ErrorMessage") is None:
                if self.table is None:
                    return self.result["CurrentStationIds"]
                else:
                    page_index = 0
                    items = []
                    self.checkTable()
                    pages = Pages(self, self.result)
                    while not self.result["Ready"] or page_index < pages.pages:
                        if page_index < pages.pages:
                            next_page = pages.fetch(page_index)
                            for item in next_page:
                                items.append(item)
                            page_index += 1
                        if page_index == pages.pages:
                            self.checkTable()
                            pages = Pages(self, self.result)
                    return items
            else:
                raise Exception(
                    "Query ended with an error: " + self.result["ErrorMessage"]
                )

    def pages(self):
        if self.token is None:
            self.token = login()
        if self.key is None:
            self.createQuery()
        else:
            self.readQuery()
        if self.table is None:
            raise Exception("Query should include a table.")
        if self.key is not None:
            self.waitQuery()
            if self.result.get("ErrorMessage") is None:
                self.checkTable()
                while not self.result["Ready"]:
                    time.sleep(1)
                    self.checkTable()
                return Pages(self, self.result)
            else:
                raise Exception(
                    "Query ended with an error: " + self.result["ErrorMessage"]
                )

    def map(self, item_func=lambda c: c):
        if self.token is None:
            self.token = login()
        if self.key is None:
            self.createQuery()
        else:
            self.readQuery()
        if self.key is not None:
            self.checkQuery()
            if self.result.get("ErrorMessage") is None:
                if self.table is None:
                    self.waitQuery()
                    for st_id in self.result["CurrentStationIds"]:
                        item_func(st_id)
                else:
                    page_index = 0
                    self.checkTable()
                    pages = Pages(self, self.result)
                    while not self.result["Ready"] or page_index < pages.pages:
                        if page_index < pages.pages:
                            next_page = pages.fetch(page_index)
                            for item in next_page:
                                yield item_func(item)
                            page_index += 1
                        if page_index == pages.pages:
                            self.checkTable()
                            pages = Pages(self, self.result)
            else:
                raise Exception(
                    "Query ended with an error: " + self.result["ErrorMessage"]
                )
            
    def getDataFrame(self, n_jobs=None):
        """Loops over Pages returned by Query, and builds a list
            Args:
            n_jobs - Number of threads to use for downloading result.
                     If None is specified it's set to number of pages in the result.
        """
        def page_parser(pages_obj, page_no):
            """Parse a single page from a pages object and return a dataframe."""
            return json_normalize(pages_obj.fetch(page_no))

        pages = self.pages()
        n_pages = pages.pages

        if n_jobs is None:
            if n_pages > 0: 
                n_jobs = n_pages
            else:
                n_jobs = 1

        with yaspin(spinner=Spinners.shark, text="Waiting for Query to finish..."):
            # Iterate over cache and build dataframe
            list = Parallel(n_jobs=n_jobs, prefer="threads")(
                delayed(page_parser)(pages, page) for page in range(n_pages)
            )
            df = pd.concat(list, axis="rows")
            return df
    

    def export(self, format, filename, where = None):
        """Create a new Archive on server, containing the export file.
           Args:
           format: One of AquaMonitor's export formats: excel, csv etc.
           filename: Filename to use for the export.
           where: If it differs from the one used to query stations.
        """
        if self.token is None:
            self.token = login()
        if self.key is None:
            self.createQuery()
        else:
            self.readQuery()
        if self.key is not None:
            self.waitQuery()
            if self.result.get("ErrorMessage") is None:
                return Archive(
                    format,
                    filename,
                    token=self.token,
                    stations=self.result["CurrentStationIds"],
                    where=self.where if where is None else where,
                )
            else:
                raise Exception(
                    "Query ended with an error: " + self.result["ErrorMessage"]
                )

    def checkQuery(self):
        resp = getJson(self.token, cache_site + "/query/" + self.key)
        if not resp.get("Result") is None:
            while not resp["Result"]["Ready"]:
                time.sleep(1)
                resp = getJson(self.token, cache_site + "/query/" + self.key)
            self.result = resp["Result"]
        else:
            raise Exception("Query didn't respond properly.")

    def checkTable(self):
        resp = getJson(self.token, cache_site + "/query/" + self.key + "/" + self.table)
        if not resp.get("Ready") is None:
            self.result = resp
        else:
            raise Exception(
                "Query didn't respond properly for table request: " + self.table
            )

    def waitQuery(self):
        resp = getJson(self.token, cache_site + "/query/" + self.key)
        if not resp.get("Result") is None:
            with yaspin(spinner=Spinners.shark, text="Waiting for Query..."):
                while not resp["Result"]["Ready"]:
                    time.sleep(1)
                    resp = getJson(self.token, cache_site + "/query/" + self.key)
                if self.table is None:
                    self.result = resp["Result"]
                else:
                    self.checkTable()
        else:
            raise Exception("Query didn't respond properly.")


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

        if not(path.endswith("/") or path.endswith("\\")) :
            path += os.sep

        if self.id is not None:
            with yaspin(spinner=Spinners.shark, text="Waiting for Export to finish..."):
                resp = getArchive(self.token, self.id)
                while resp.get("Archived") is None:
                    time.sleep(5)
                    resp = getArchive(self.token, self.id)

                if not resp.get("ErrorMessage") is None:
                    raise Exception(resp["ErrorMessage"])

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

def get_project_chemistry_input(proj_id, st_dt, end_dt, token=None, n_jobs=None):
    """Get all water chemistry data for the specified project ID and date range.
    Args:
        proj_id:  Int.
        st_dt:    Str. Start of period of interest in format 'dd.mm.yyyy'
        end_dt:   Str. End of period of interest in format 'dd.mm.yyyy'
        token:    Str. Optional. Valid API access token. If None, will first attempt to read
                  credentials from a '.auth' file in the installation folder. If this fails,
                  will prompt for username and password
        n_jobs:   None or int. Number of threads to use for fetching query results in
                  parallel. If None (default) the number of threads is equal to the number
                  of pages in the server response, which is usually a sensible choice
    Returns:
        Dataframe.
    """
    if not token:
        token = login()

    # Query API and save result-set to cache
    where = (
        f"project_id = {proj_id} and sample_date >= {st_dt} and sample_date <= {end_dt}"
    )
    table = "water_chemistry_input"
    query = Query(where=where, token=token, table=table)

    df = query.getDataFrame(n_jobs)

    if "Flag" not in df.columns:
        df["Flag"] = None

    columns_to_drop = [
        '$type', 'Id', 'Sample.Method.Code', 'QuantificationLimit',
        'Accredited', 'Accreditation', 'Sample.Method.Id', 'Flag', 'Remark', 'Method.Id'
    ]

    # Drop the specified columns, ignoring any that are not in the DataFrame
    df = df.drop(columns=columns_to_drop, errors='ignore')

    df.rename(columns = {
        "Sample.Id": "sample_id",
        "Sample.Station.Project.Id": "project_id",
        "Sample.Station.Project.Name": "project_name",
        "Sample.Station.Id": "station_id",
        "Sample.Station.Code": "station_code",
        "Sample.Station.Name": "station_name",
        "Sample.SampleDate": "sample_date",
        "Sample.Depth1": "depth1",
        "Sample.Depth2": "depth2",
        "Method.Name": "parameter_name",
        'Method.Unit': 'method_unit',
        'Method.Laboratory' : "method_laboratory",
        'Method.MethodRef': "method_ref",
        "Value": "value",

    }, inplace = True)


    df["sample_date"] = pd.to_datetime(df["sample_date"])

    df = df.reindex(df.columns.union(["project_id",
            "project_name",
            "station_id",
            "station_code",
            "station_name",
            "sample_date",
            "depth1",
            "depth2",
            "parameter_name",
            "flag",
            "value",
            "method_unit"   
        ], sort= False), axis="columns")

    df.sort_values(
        [
            "project_id",
            "station_id",
            "sample_date",
            "depth1",
            "depth2",
            "parameter_name",
        ],
        inplace=True,
    )
    df.reset_index(inplace=True, drop=True)

    nans = df.parameter_name.isna()
    if nans.any():
        empty_names = df[nans]
        print("Rows with empty 'parameter_name' values:")
        print(empty_names['sample_date'].values)
        df = df[~nans]


    return df

def get_project_chemistry(proj_id, st_dt, end_dt, token=None, n_jobs=None):
    """Get all water chemistry data for the specified project ID and date range.
    Args:
        proj_id:  Int.
        st_dt:    Str. Start of period of interest in format 'dd.mm.yyyy'
        end_dt:   Str. End of period of interest in format 'dd.mm.yyyy'
        token:    Str. Optional. Valid API access token. If None, will first attempt to read
                  credentials from a '.auth' file in the installation folder. If this fails,
                  will prompt for username and password
        n_jobs:   None or int. Number of threads to use for fetching query results in
                  parallel. If None (default) the number of threads is equal to the number
                  of pages in the server response, which is usually a sensible choice
    Returns:
        Dataframe.
    """
    if not token:
        token = login()

    # Query API and save result-set to cache
    where = (
        f"project_id = {proj_id} and sample_date >= {st_dt} and sample_date <= {end_dt}"
    )
    table = "water_chemistry_output"
    query = Query(where=where, token=token, table=table)
    df = query.getDataFrame(n_jobs)

    if "Flag" not in df.columns:
        df["Flag"] = None

    df.rename(columns = {
        "Sample.Station.Project.Id": "project_id",
        "Sample.Station.Project.Name": "project_name",
        "Sample.Station.Id": "station_id",
        "Sample.Station.Code": "station_code",
        "Sample.Station.Name": "station_name",
        "Sample.SampleDate": "sample_date",
        "Sample.Depth1": "depth1",
        "Sample.Depth2": "depth2",
        "Parameter.Name": "parameter_name",
        "Flag": "flag",
        "Value": "value",
        "Parameter.Unit": "unit"
    }, inplace = True)

    df["sample_date"] = pd.to_datetime(df["sample_date"])

    df = df.reindex(df.columns.union(["project_id",
            "project_name",
            "station_id",
            "station_code",
            "station_name",
            "sample_date",
            "depth1",
            "depth2",
            "parameter_name",
            "flag",
            "value",
            "unit"   
        ], sort= False), axis="columns")
    
    df.sort_values(
        [
            "project_id",
            "station_id",
            "sample_date",
            "depth1",
            "depth2",
            "parameter_name",
        ],
        inplace=True,
    )
    df.reset_index(inplace=True, drop=True)

    return df

def long_to_wide(df_long):
    """ Converts into a wide format df from get_project_chemistry_input """
    
    units = df_long[['parameter_name', 'method_unit']].drop_duplicates()
    d = df_long[df_long.duplicated(
        subset=[
            "parameter_name",
            "sample_date",
            "project_name",
            "station_name",
            "project_id",
            "station_id",
            "station_code",
            "depth1",
            "depth2",
        ],
        keep=False,
    )]

    if len(d) > 0:
        print(f"Warning: found duplicates in Aqm for {df_long.project_name.values[0], d}")       
        raise ValueError("Found duplicated data")



    df_wide = df_long.pivot(
            columns=["parameter_name"],
            index=[
                "sample_date",
                "project_name",
                "station_name",
                "project_id",
                "station_id",  # 'unit',
                "station_code",
                "depth1",
                "depth2",
            ],
            values="value",
        )
        
    return df_wide, units


def extract_o_numbers(row):
    """Return a commaseparated list of o-numbers.
       Based on the json structure: O_Numbers[{Id, Number}]
    """
    numbers = row["O_Numbers"]
    if isinstance(numbers, float):
        return

    return ",".join([n["Number"] for n in numbers])


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
    df["project_code"] = df.apply(extract_o_numbers, axis=1)
    df.rename(
        {
            "Id": "project_id",
            "Name": "project_name",
            "Description": "description",
        },
        inplace=True,
        axis="columns",
    )
    df = df[
        [
            "project_id",
            "project_code",
            "project_name",
            "description",
        ]
    ]
    df.sort_values(["project_id"], inplace=True)
    df.reset_index(inplace=True, drop=True)

    return df





def get_project_stations(proj_id, token=None, return_coords=True, n_jobs=None):
    """Get stations associated with a specific project.

    Args:
        proj_id:       Int. Project ID for project of interest
        token:         Str. Optional. Valid API access token. If None, will first attempt to read
                       credentials from a '.auth' file in the installation folder. If this fails,
                       will prompt for username and password
        return_coords: Bool. Whether to include latitudes and longitudes for stations in the result.
                       Default True. Setting to False will make the query significantly faster
        n_jobs:        None or int. Number of threads to use for fetching query results in
                       parallel. If None (default) the number of threads is equal to the number
                       of pages in the server response, which is usually a sensible choice

    Returns:
        Dataframe
    """

    # Get basic station data
    if not token:
        token = login()

    resp = getJson(token, aqua_site + f"/api/projects/{proj_id}/stations")
    df = json_normalize(resp)

    # Tidy
    del df["Type.Id"]
    df.rename(
        {
            "Id": "station_id",
            "Project.Id": "project_id",
            "Code": "station_code",
            "Name": "station_name",
            "Type.Text": "type",
        },
        inplace=True,
        axis="columns",
    )
    df = df[["project_id", "station_id", "station_code", "station_name", "type"]]
    df.sort_values(["project_id", "station_id"], inplace=True)
    df.reset_index(inplace=True, drop=True)

    if return_coords:
        where = f"project_id = {proj_id}"
        table = "metadata"
        query = Query(where=where, token=token, table=table)
        coord_df = query.getDataFrame(n_jobs)
        coord_df = coord_df[["_Id", "_Longitude", "_Latitude"]]
        coord_df.columns = ["station_id", "longitude", "latitude"]

        # Join
        df = pd.merge(df, coord_df, on="station_id", how="left")

    return df

def get_water_parameters(name=None, token=None):
    """Get list of water chemistry parameters.

    Args:
         name - Constrain list to those parameters starting with the given text.
                If not specified return all parameters.
         token - User credentials. If not specified will try to get them.
    """
    
    if token is None:
        token = login()

    if name is None:
        name = "%"

    resp = getJson(token, aqua_site + f"/api/query/parameter?datatype=Water&name={name}")
    df = json_normalize(resp)

    # Tidy
    df.rename(
        {
            "Id": "id",
            "Name": "name",
            "Unit": "unit",
        },
        inplace=True,
        axis="columns",
    )
    df = df[
        [
            "id",
            "name",
            "unit"
        ]
    ]
    df.sort_values(["name"], inplace=True)
    df.reset_index(inplace=True, drop=True)
    return df

def get_station_attributes(stat_ids, token = None):
    """Get table of station attributes for a set of station id's.

    Args:
        stat_ids: Array of station id's.
        token: User credentials. If not specified will try to get them.
               User must have read access to the stations provided.
    """

    if token is None:
        token = login()
    
    query = Query(stations=stat_ids, token=token, table="station_attributes")
    df = query.getDataFrame()
    df_pivot = df.pivot(index='Station.Id', columns='Attribute.Name', values=['NumericValue','TextValue'])
    df_pivot.dropna(axis=1, how='all', inplace=True)
    df_pivot = pd.merge(df_pivot['TextValue'], df_pivot['NumericValue'], left_on='Station.Id', right_on='Station.Id')
    return df_pivot

def get_station_types(token = None):
    """Get list of all station types. 
    
    Args:
        token: User credentials. If not specified will try to get them.
               No specific requirement to the user.
    """

    if token is None:
        token = login()

    resp = getJson(token, aqua_site + "/api/stationtypes")
    df = json_normalize(resp)

    # Tidy
    df.rename(
        {
            "Id": "id",
            "Text": "text"
        },
        inplace=True,
        axis="columns",
    )
    df.sort_values(["text"], inplace=True)
    df.reset_index(inplace=True, drop=True)
    return df

def get_water_samples(token = None, proj_id = None, stat_ids = None, from_date = None, to_date = None):
    """Get list of water samples.
        Project or an array of stations must be provided. A period can be provided in addition.
    Args:
    token: User credentials. If not specified will try to get them.
           User must have access rights to the project or stations.
    proj_id: Project Id. If provided will use stations of the project.
    stat_ids: Array of station Id. proj_id can't be given together with stations.
    from_date: Start of period
    to_date: End of period
    """

    if proj_id is None and stat_ids is None:
        raise Exception("You must provide either proj_id or stat_ids.")
    
    if token is None:
        token = login()

    where = None
    if not proj_id is None:
        where = "project_id=" + proj_id

    if not from_date is None:
        if not where is None:
            where += " and sample_date>" + from_date
        else:
            where = "sample_date>" + from_date
    
    if not to_date is None:
        if not where is None:
            where += " and sample_date<" + to_date
        else:
            where = "sample_date>" + to_date
    
    query = Query(stations=stat_ids, where=where, token=token, table="water_samples")
    df = query.getDataFrame()
    df.rename(columns = {
        "Id": "sample_id",
        "Station.Id": "station_id",
        "SampleDate": "sample_date",
        "Depth1": "depth1",
        "Depth2": "depth2"
    }, inplace = True)

    df["sample_date"] = pd.to_datetime(df["sample_date"])
    df = df[["sample_id", "station_id", "sample_date", "depth1", "depth2"]]
    df.reset_index(inplace=True, drop=True)

    return df