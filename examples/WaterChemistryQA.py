import aquamonitor as am
import pandas as pd

#am.host = "https://test-aquamonitor.niva.no/"


def openQuery(station_id, year):
    query = am.Query(where=f"sample_date>=01.01.{year} and sample_date<=31.12.{year}",
                     stations=[station_id],
                     table="water_chemistry_input")
    query.createQuery()
    return query.key


def printQuery(key):
    am.Query(key=key).map(print)


def filterMethod(key, name):
    return filter(lambda c: c["Method"]["Name"] == name, am.Query(key=key).map())


def approveValues(name, dates):
    for value in filter(lambda wc: wc["Method"]["Name"] == name and wc["Sample"]["SampleDate"] in dates,
                        am.Query(key=key).map()):
        value["Approved"] = True
        value["Remark"] = ""
        am.putJson(token, am.aqua_site + f"/api/waterchemistry/{value['Id']}", value)
        print(value)

def disapproveValues(name, dates):
    for value in filter(lambda wc: wc["Method"]["Name"] == name and wc["Sample"]["SampleDate"] in dates,
                        am.Query(key=key).map()):
        value["Approved"] = False
        value["Remark"] = "OKA: Tas ut"
        print(am.putJson(token, am.aqua_site + f"/api/waterchemistry/{value['Id']}", value))

def iterateMethod(col, name):
    dates=[]
    for idx, row in table.iterrows():
        if not pd.isna(row[col]):
            dates.append(row[1].strftime("%Y-%m-%dT%XZ"))
    disapproveValues(name, dates)

year = "2020"
station_name = "Målselva"
station_id = 70881

token = am.login()
key = openQuery(station_id, year)
table = pd.read_excel("K:\\Prosjekter\\Land-sea interactions\\_Loggedata fra LOI-stasjonene\\"+
                      f"_{station_name}\\" +
              f"Korreksjoner_ikke oppdatert i AM\\{station_name}_sensordata_{year}_inkl figs.xlsx",
                      "WaterChemistry", header=1)

iterateMethod(3, "Fluorescens")
iterateMethod(5, "Kond")
iterateMethod(7, "pH")
iterateMethod(9, "Temp")
iterateMethod(11, "Turb")
iterateMethod(13, "CDOManalogFinal")
#iterateMethod(11, "Vannstand")

#for v in am.Query(key=key).map():
#    print(v)