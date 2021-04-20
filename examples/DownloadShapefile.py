__author__ = 'Roar Brenden'

import aquamonitor as am
import datetime
import zipfile


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
    resp = am.createDatafile(token, data)
    return resp["Id"]


def download_file(id, filename, path) :
    archived = False
    while not archived:
        resp = am.getArchive(token, id)
        archived = resp.get("Archived")
    am.downloadArchive(token, id, filename, path)


expires = datetime.date.today() + datetime.timedelta(days=1)

#am.host = "https://test-aquamonitor.niva.no/"
token = am.login()

filename = 'Referanseelver2017.zip'
projectId = 11226

where = "project_id=" + str(projectId)
stations = am.getStations(token, projectId)
stationids = []

for st in stations:
    stationids.append(st.get("Id"))
print(stationids)

archive = make_shapefile("Referanseelver", filename, stationids, where)

root = 'c:/Temp/'
path = root + filename

download_file(archive, filename, path)

am.deleteArchive(token, archive)

zipFile = zipfile.ZipFile(path, 'r')
zipFile.extractall(root)
zipFile.close()
