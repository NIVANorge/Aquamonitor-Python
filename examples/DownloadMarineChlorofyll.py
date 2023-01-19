# Find some Id's from NIVADATABASE
#
# From station_types :  station_type_id = 3
#
# From wc_parameter_definitions : Water.parameter_id = 261

import aquamonitor as am
import datetime

# We need some additional functions
#
#  Start generating Excel file at the server
def make_excel(title, stationids, where):
    expires = datetime.date.today() + datetime.timedelta(days=1)
    data = {
        "Expires": expires.strftime("%Y.%m.%d"),
        "Title": title,
        "Files": [{"Filename": title + '.xlsx', "ContentType": "application/vnd.ms-excel"}],
        "Definition": {"Format": "excel", "StationIds": stationids, "DataWhere": where},
    }
    resp = am.createDatafile(token, data)
    return resp["Id"]

#  Check if the file is ready and download to path
def download_file(id, filename, path):
    archived = False
    while not archived:
        resp = am.getArchive(token, id)
        archived = resp.get("Archived")
    am.downloadArchive(token, id, filename, path)

token = am.login()
where = "station_type_id = 3 and Water.parameter_id = 261"
stids = am.Query(where).list()
fil = make_excel("Klfa", stids, where)
download_file(fil, "Klfa.xlsx", "C:/Temp/Klfa.xlsx")
