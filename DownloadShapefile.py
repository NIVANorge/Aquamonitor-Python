__author__ = 'Roar Brenden'

import AquaMonitor
import datetime
import zipfile

expires = datetime.date.today() + datetime.timedelta( days=1 )

def make_shapefile(title, filename, stationids, where) :

    data = {
        "Expires": expires.strftime('%Y.%m.%d'),
        "Title": title,
        "Files":[{
            "Filename": filename,
            "ContentType":"application/zip"}],
        "Definition":{
            "Format":"shape",
            "StationIds": stationids,
            "DataWhere": where
        }
    }
    resp = AquaMonitor.createDatafile(token, data)
    return resp["Id"]


def download_file(id, filename, path) :
    archived = False
    while not archived:
        resp = AquaMonitor.getArchive(token, id)
        archived = resp.get("Archived")
    AquaMonitor.downloadArchive(token, id, filename, path)

token = AquaMonitor.login()

filename = 'Referanseelver2017.zip'
projectId = 11226

where = "project_id=" + str(projectId)
stations = AquaMonitor.getStations(token, projectId)
stationids = []

for st in stations:
    stationids.append(st.get("Id"))
print(stationids)

archive = make_shapefile("Referanseelver", filename, stationids, where)

root = '/Users/roar/'
path = root + filename

download_file(archive, filename, path)

#AquaMonitor.deleteArchive(token, archive)

zipFile = zipfile.ZipFile(path, 'r')
zipFile.extractall(root)
zipFile.close()
