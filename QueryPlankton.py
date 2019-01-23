__author__ = 'Roar Brenden'
import AquaMonitor as am
import datetime as dt

am.host = "http://aquamon.niva.corp/"
am.aqua_site = "AquaCache"

path = "AquaCache/query/"
token = am.login()

station_years = {}
station_projects = {}
project_name_count = {}
query_keys = []


def start_query(year) :
    where = "station_type_id=1 and datatype=Plankton and sample_date>=01.01." + str(year) +\
            " and sample_date<01.01." + str(year + 1)
    query = {
             "StationWhere": where,
             "From": {
                     "Table": "Stations"
                 }
            }

    resp = am.postJson(token, path, query)
    query_keys.append(resp["Key"])

def get_stations(key, year) :
    ready = False
    while not ready:
        resp = am.getJson(token, path + key + "/stations/")
        ready = resp["Detail"]["Ready"]
    stations = resp["Result"]
    am.delete(token, path + key)

    for stat in stations:
        stid = stat["StationId"]
        if station_years.has_key(stid):
            if not station_years[stid][len(station_years[stid]) - 1] == year:
                station_years[stid].append(year)
        else:
            station_years[stid] = [year]
            station_projects[stid] = []

        if len(station_years[stid]) == 1:
            station_projects[stid].append(stat["ProjectId"])

        if not stat["ProjectId"] in project_name_count.keys():
            project_name_count[stat["ProjectId"]] = {"Name": stat["ProjectName"], "Count": 0}


def make_file(title, filename, stids, prid) :
    expires = dt.date.today() + dt.timedelta(days=1)

    where = "sample_date>01.01.1990 and sample_date<01.01.2017 and datatype=Plankton" + \
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
    mx = -1
    prid = -1
    for id in project_name_count.keys():
        if project_name_count[id]["Count"] > mx:
            mx = project_name_count[id]["Count"]
            prid = id
    return prid

for y in range(1990, 2016):
    start_query(y)

for key in query_keys:
    get_stations(key, 1990 + query_keys.index(key))

for stid in station_years.keys():
    if len(station_years[stid]) < 5:
        del station_years[stid]
        del station_projects[stid]


for stid in station_projects.keys():
    for prid in station_projects[stid]:
        project_name_count[prid]["Count"] += 1


while len(project_name_count) > 0:
    prid = find_next_project()
    stids = []
    for stid in station_projects.keys():
        if prid in station_projects[stid]:
            stids.append(stid)
            for p in station_projects[stid]:
                project_name_count[p]["Count"] -= 1
            del station_projects[stid]
    if len(stids) > 0:
        print(make_file("Plankton " + project_name_count[prid]["Name"], \
                        "plankton_" + str(prid) + "_1990_2016.xlsx", \
                        stids, \
                        prid))

    del project_name_count[prid]