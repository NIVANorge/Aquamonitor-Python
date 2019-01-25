__author__ = 'Roar Brenden'
import AquaMonitor as am
import datetime as dt

am.host = "http://aquamon.niva.corp/"
am.aqua_site = "AquaCache"

path = am.aqua_site + "/query/"
token = am.login()

station_years = {}
station_projects = {}
project_name_count = {}
query_keys = []


def start_query(year, datatype) :
    where = "station_type_id=1 and datatype=" + datatype + " and sample_date>=01.01." + str(year) +\
            " and sample_date<01.01." + str(year + 1)
    query = {
             "WhereStation": where,
             "From": [{
                     "Table": "Stations"
                 }]
            }

    resp = am.postJson(token, path, query)
    query_keys.append(resp["Key"])

def get_stations(key, year) :
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

    stations = resp["Items"]
    am.deleteJson(token, path + key)

    for stat in stations:
        stid = stat["Id"]
        if stid in station_years:
            if not station_years[stid][len(station_years[stid]) - 1] == year:
                station_years[stid].append(year)
        else:
            station_years[stid] = [year]
            station_projects[stid] = []

        if len(station_years[stid]) == 1:
            station_projects[stid].append(stat["ProjectId"])

        if not stat["ProjectId"] in project_name_count.keys():
            project_name_count[stat["ProjectId"]] = {"Count": 0}


def make_file(title, filename, datatype, stids, prid) :
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
    mx = -1
    prid = -1
    for id in project_name_count.keys():
        if project_name_count[id]["Count"] > mx:
            mx = project_name_count[id]["Count"]
            prid = id
    return prid



datatype = "Vannplanter"

for y in range(1990, 2016):
    start_query(y, datatype)

for key in query_keys:
    get_stations(key, 1990 + query_keys.index(key))

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
        print(make_file(datatype + " " + str(prid),
                        datatype + "_" + str(prid) + "_1990_2016.xlsx",
                        datatype,
                        stids,
                        prid))
    del project_name_count[prid]
