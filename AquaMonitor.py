__author__ = 'Roar Brenden'

import requests
from xml.dom import minidom
import json
import configparser
import pyexpat
import time
import datetime

host = 'http://www.aquamonitor.no/'
aqua_site = 'AquaServices'
archive_site = 'AquaServices'
cache_site = 'AquaCache'


def requestService(url, params):
    response = requests.post(url, params)
    try:
        return minidom.parseString(response.text)
    except pyexpat.ExpatError as e:
        print("URL: " + url)
        print("PARAMS:" + ''.join('{}={} '.format(key, val) for key, val in params.items()))
        print("RESPONSE:" + response.text)
        raise e


def login(username=None, password=None):
    if username is None:
        config = configparser.RawConfigParser()
        try:
            config.read(".auth")
            username = config.get("Auth", "username")
            password = config.get("Auth", "password")
        except Exception as ex:
            raise Exception("Couldn't read username/password in file .auth")

    loginurl = host + aqua_site + '/WebServices/LoginService.asmx/AuthenticateUser'

    loginparams = {'username': username, 'password': password}

    userdom = requestService(loginurl, loginparams)

    usertype = userdom.getElementsByTagName('Usertype')[0].childNodes[0].nodeValue

    if not usertype == 'NoUser':
        token = userdom.getElementsByTagName('Token')[0].childNodes[0].nodeValue
    else:
        token = None

    return token


def get(token: str, site: str, path: str, stream: bool = False):
    return requests.get(host + site + path, cookies=dict(aqua_key=token), stream=stream)


def reportJsonError(response):
    message = "AquaMonitor failed with status: " + response.reason + " and message:"
    if response.text is not None:
        try:
            message = message + "\n\n" + json.loads(response.text).get("Message")
        except:
            message = message + "\nDidn't send JSON."

    raise Exception(message)


def getJson(token, path):
    response = requests.get(host + path, cookies=dict(aqua_key=token))
    if response.status_code == 200:
        return json.loads(response.text)
    else:
        reportJsonError(response)


def postJson(token, path, inJson):
    response = requests.post(host + path, json=inJson, cookies=dict(aqua_key=token))
    if response.status_code == 200:
        return json.loads(response.text)
    else:
        reportJsonError(response)


def putJson(token, path, inJson):
    response = requests.put(host + path, json=inJson, cookies=dict(aqua_key=token))
    return json.loads(response.text)


def deleteJson(token, path):
    response = requests.delete(host + path, cookies=dict(aqua_key=token))
    return json.loads(response.text)


def getProject(token, projectId):
    projectsurl = 'AquaServices/api/projects/' + str(projectId)
    return getJson(token, projectsurl)


def getStations(token, projectId):
    stationsurl = 'AquaServices/api/projects/' + str(projectId) + '/stations/'
    return getJson(token, stationsurl)


def getArchive(token, id):
    path = archive_site + '/files/archive/' + id
    return getJson(token, path)


def createDatafile(token, data):
    path = archive_site + '/files/datafile/'
    return postJson(token, path, data)


def deleteArchive(token, id):
    path = archive_site + '/files/archive/' + id
    return deleteJson(token, path)


def downloadFile(token: str, url: str, path: str) -> None:
    resp = get(token, "", url)
    with open(path, 'wb') as fd:
        for chunk in resp.iter_content(chunk_size=256):
            fd.write(chunk)


def downloadArchive(token, id, file, path):
    downloadFile(token, archive_site + "/files/archive/" + id + '/' + file, path)


class Query:
    token = None
    key = None
    table = None

    result = None

    def __init__(self):
        self.where = None

    def __init__(self, where):
        self.where = where

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
                    return self.result["Items"]
            else:
                raise Exception("Query ended with an error: " + self.result["ErrorMessage"])

    def makeArchive(self, fileformat, filename):
        if self.token is None:
            self.token = login()

        if self.key is None:
            self.createQuery()

        if not self.key is None:
            self.waitQuery()
            if self.result.get("ErrorMessage") is None:
                return Archive(fileformat, filename,
                               token=self.token,
                               stations=self.result["CurrentStationIds"],
                               where=self.where)
            else:
                raise Exception("Query ended with an error: " + self.result["ErrorMessage"])

    def waitQuery(self):
        resp = getJson(self.token, cache_site + "/query/" + self.key)

        if not resp.get("Result") is None:
            while not resp["Result"]["Ready"]:
                time.sleep(1)
                resp = getJson(self.token, cache_site + "/query/" + self.key)
            if self.table is None:
                self.result = resp["Result"]
            else:
                resp = getJson(self.token, cache_site + "/query/" + self.key + "/" + self.table)
                if not resp.get("Ready") is None:
                    while not resp["Ready"]:
                        time.sleep(1)
                        resp = getJson(self.token, cache_site + "/query/" + self.key + "/" + self.table)
                    self.result = resp
                else:
                    raise Exception("Query didn't respond properly for table request: " + self.table)
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
                downloadArchive(self.token, self.id, file["FileName"], path + file["FileName"])
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
            "Expires": self.expires.strftime('%Y.%m.%d'),
            "Title": "QueryExample",
            "Files": [{
                "Filename": self.filename,
                "ContentType": content_type}],
            "Definition": {
                "Format": self.fileformat,
                "StationIds": self.stations,
                "DataWhere": self.where
            }
        }
        resp = createDatafile(self.token, archive)
        if not resp.get("Id") is None:
            self.id = resp["Id"]

class Graph:
    token = None
    site = None
    url = None

    def __init__(self, width : int, height : int, **kwargs):
        self.token = kwargs.get("token")
        self.site = kwargs.get("site")

        self.url = kwargs.get("graph") + "?w=" + str(width) + "&h=" + str(height) \
        + "&stid=" + str(kwargs.get("stationId")) + "&p=" + kwargs.get("parameter") \
        + "&where=" + kwargs.get("where")

    def download(self, path: str):
        response = get(self.token, self.site, self.url, stream=True)
        if response.status_code == 200:
            with open(path, 'wb') as file:
                for chunk in response.iter_content():
                    file.write(chunk)
