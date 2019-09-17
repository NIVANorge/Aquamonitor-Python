import AquaMonitor as am
import datetime

am.host = "http://localhost:59265"
am.aqua_site = ""
am.archive_site = ""

where = "project_id=12428"
title = "DuPont"
filename = "dupont.xlsx"

expires = datetime.date.today() + datetime.timedelta(days=1)

token = am.login()

data = {
    "Expires": expires.strftime('%Y.%m.%d'),
    "Title": title,
    "Files":[{
        "Filename": filename,
        "ContentType":"application/vnd.ms-excel"}],
    "Definition":{
        "Format":"excel",
        "StationIds": [71674, 71675, 71676, 71672, 71673],
        "DataWhere": where
    }
}
print(am.createDatafile(token, data))