__author__ = 'Roar Brenden'


import aquamonitor.aquamonitor as am
import datetime

#root = "/Users/roar/"
root = "C:/Temp/"
#am.host = "https://test-aquamonitor.niva.no/"

fromDate = datetime.date.today() + datetime.timedelta( days = -150 )
toDate = datetime.date.today() + datetime.timedelta( days = -1 )
expires = datetime.date.today() + datetime.timedelta( days=1 )
token = am.login()

def make_file(title, filename, stationid, datatype) :
    where = "sample_date>=" + datetime.datetime.strftime(fromDate, '%d.%m.%Y') \
            + " and sample_date<" + datetime.datetime.strftime(toDate, '%d.%m.%Y') \
            + " and datatype=" + datatype

    data = {
        "Expires": expires.strftime('%Y.%m.%d'),
        "Title": title,
        "Files":[{
            "Filename": filename,
            "ContentType":"text/csv"}],
        "Definition":{
            "Format":"csv",
            "StationIds": [ stationid ],
            "DataWhere": where
        }
    }
    resp = am.createDatafile(token, data)
    return resp["Id"]


def download_file(id, filename) :
    archived = False
    while not archived:
        resp = am.getArchive(token, id)
        archived = resp.get("Archived")
    path = root + filename
    am.downloadArchive(token, id, filename, path)


weatherFileId = make_file('Langtjern weather', 'langtjernWeather.csv', 62040, 'Air')
lakeFileId = make_file('Langtjern lake', 'langtjernLake.csv', 50472, 'Water')
outletFileId = make_file('Langtjern outlet', 'langtjernOutlet.csv', 51356, 'Water')
inletFileId = make_file('Langtjern inlet', 'langtjernInlet.csv', 63098, 'Water')

download_file(weatherFileId, 'langtjernWeather.csv')
download_file(lakeFileId, 'langtjernLake.csv')
download_file(inletFileId, 'langtjernInlet.csv')
download_file(outletFileId, 'langtjernOutlet.csv')
