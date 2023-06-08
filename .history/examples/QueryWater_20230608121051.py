import aquamonitor as am
import datetime as dt

params = ["Water.parameter_id=11","Water.parameter_id=261","Water.parameter_id=2097","Water.parameter_id=1621","Water.parameter_id=301","Water.parameter_id=13"]

#am.host = "https://test-aquamonitor.niva.no/"

path = "AquaCache/query/"
token = am.login()

station_years = {}
station_projects = {}
project_name_count = {}
query_keys = []

def start_query(year) :
    where = "station_type_id=1 and datatype=Plankton and sample_date>01.01." + str(year) + " and sample_date<01.01." + str(
            year + 1)

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
        resp = am.getJson(token, path + key + "/Stations/")
        if "Ready" in resp:
            ready = resp["Ready"]
        else:
            print(resp)

    pages = resp["Pages"]
    for i in range(pages):
        stationsResponse = am.getJson(token, path + key + "/Stations/" + str(i))
        stations = stationsResponse["Items"]
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


def make_file(title, filename, stids, prid) :
    expires = dt.date.today() + dt.timedelta(days=1)
    where = ""
    part = " or sample_date>01.01.1990 and sample_date<01.01.2017 and project_id=" + str(prid)

    for p in params:
        where = where + part + " and " + p

    data = {
        "Expires": expires.strftime('%Y.%m.%d'),
        "Title": title,
        "Files":[{
            "Filename": filename,
            "ContentType":"application/vnd.ms-excel"}],
        "Definition":{
            "Format":"excel",
            "StationIds": stids,
            "DataWhere": where[4:]
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

original_keys = list(station_years.keys())
for stid in original_keys:
    if len(station_years[stid]) < 5:
        del station_years[stid]
        del station_projects[stid]

original_keys = list(station_projects.keys())
for stid in original_keys:
    for prid in station_projects[stid]:
        project_name_count[prid]["Count"] += 1


while len(project_name_count) > 0:
    prid = find_next_project()
    stids = []

    original_keys = list(station_projects.keys())
    for stid in original_keys:
        if prid in station_projects[stid]:
            stids.append(stid)
            for p in station_projects[stid]:
                project_name_count[p]["Count"] -= 1
            del station_projects[stid]
    if len(stids) > 0:
        print(make_file("WaterChemistry " + str(prid), "waterchemistry" + str(prid) + "_1990_2016.xlsx", stids, prid))
    del project_name_count[prid]