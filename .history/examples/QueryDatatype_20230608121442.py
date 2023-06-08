__author__ = 'Roar Brenden'
import aquamonitor.aquamonitor as am
import datetime as dt

#am.host = "https://test-aquamonitor.niva.no/"


am.aqua_site = "AquaCache"
path = am.aqua_site + "/query/"
token = am.login()

station_years = {}
station_projects = {}
project_name_count = {}
query_keys = []


def start_query(year, datatype):
    """
    Post a query definition that finds every station with data of the given datatype in a year.
    The resulting key is appended to query_keys.
    :param year: Year to query
    :param datatype: Datatype to query
    :return: Nothing
    """
    where = "datatype=" + datatype \
            + " and sample_date>=01.01." + str(year) +\
            " and sample_date<01.01." + str(year + 1)
    query = {
             "WhereStation": where,
             "From": [{
                     "Table": "Stations"
                 }]
            }

    resp = am.postJson(token, path, query)
    query_keys.append(resp["Key"])


def get_stations(key, year):
    """
    Waits until the query for stations is finished.
    Then iterates all stations and adds to the arrays station_years and station_projects.
    Station_years says which year each station were sampled.
    Station_projects says which projects the station is part of.
    :param key: Key to the query
    :param year: Which year the query is for
    :return: Nothing
    """
    ready = False
    while not ready:
        resp = am.getJson(token, path + key + "/stations/")
        if "Ready" in resp:
            ready = resp["Ready"]
        else:
            if "ErrorMessage" in resp:
                print(resp["ErrorMessage"])
                break
            else:
                if "Message" in resp:
                    raise Exception(resp["Message"])
                else:
                    print(resp)
                raise Exception("Something was wrong!")

    for i in range(resp["Pages"]):
        statResponse = am.getJson(token, path + key + "/stations/" + str(i))
        stations = statResponse["Items"]
        for stat in stations:
            stid = stat["Id"]
            if stid in station_years:
                if not station_years[stid][len(station_years[stid]) - 1] == year:
                    station_years[stid].append(year)
            else:
                station_years[stid] = [year]
                station_projects[stid] = []
            proj_id = stat["Project"]["Id"]

            if len(station_years[stid]) == 1:
                station_projects[stid].append(proj_id)

            if not proj_id in project_name_count.keys():
                project_name_count[proj_id] = {"Count": 0}
    am.deleteJson(token, path + key)


def make_file(title, filename, stids, datatype, prid) :
    """
    Creates an Excel file with the given station-ids.
    The data are further narrowed by the datatype and project-id.
    The time period is fixed at 1990 - 2016
    :param title: Title to use in Eksport
    :param filename: Filename to use in Eksport
    :param stids: The station-ids to extract
    :param datatype: The datatype to use in the query
    :param prid: The project-id to use in the query
    :return: Id of archive in Eksport
    """
    expires = dt.date.today() + dt.timedelta(days=1)

    where = "sample_date>01.01.1990 and sample_date<01.01.2017 and datatype=" + datatype + \
            " and project_id=" + str(prid)

    data = {
        "Expires": expires.strftime('%Y.%m.%d'),
        "Title": title,
        "Files": [{
            "Filename": filename,
            "ContentType": "application/vnd.ms-excel"}],
        "Definition": {
            "Format": "excel",
            "StationIds": stids,
            "DataWhere": where
        }
    }
    resp = am.createDatafile(token, data)
    return resp["Id"]

def find_next_project():
    """
    Returns the project with most stations.
    Iterates project_name_count and returns the project with most highest "Count".
    :return: Project-id
    """
    mx = -1
    prid = -1
    for id in project_name_count.keys():
        if project_name_count[id]["Count"] > mx:
            mx = project_name_count[id]["Count"]
            prid = id
    return prid



datatype = "Vannplanter"

for y in range(1990, 2017):
    start_query(y, datatype)

for key in query_keys:
    year = 1990 + query_keys.index(key)
    get_stations(key, 1990 + query_keys.index(key))
    print("Year queried:" + str(year))

for stid in station_projects.keys():
    for prid in station_projects[stid]:
        project_name_count[prid]["Count"] += 1


while len(project_name_count) > 0:
    prid = find_next_project()
    stids = []
    for stid in list(station_projects.keys()):
        if prid in station_projects[stid]:
            stids.append(stid)
            for p in station_projects[stid]:
                project_name_count[p]["Count"] -= 1
            del station_projects[stid]
    if len(stids) > 0:
        print("Archive created:" + make_file(datatype + " " + str(prid),
                        datatype + "_" + str(prid) + "_1990_2016.xlsx",
                        stids,
                        datatype,
                        prid))
    del project_name_count[prid]
