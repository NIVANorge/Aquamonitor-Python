
# station_type_id = 3
# Water.parameter_id = 261

import aquamonitor as am
import datetime
# am.host = "https://test-aquamonitor.niva.no/"
token = am.login()

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


def download_file(id, filename, path):
    archived = False
    while not archived:
        resp = am.getArchive(token, id)
        archived = resp.get("Archived")
    am.downloadArchive(token, id, filename, path)

where = "station_type_id = 3 and Water.parameter_id = 261"

stids = am.Query(where).list()
fil = make_excel("Klfa", stids, where)
download_file(fil, "Klfa.xlsx", "C:/Temp/Klfa.xlsx")
