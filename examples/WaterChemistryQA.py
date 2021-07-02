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
    for value in filter(lambda wc: wc["Method"]["Name"] == name
                                   and wc["Sample"]["Depth1"] == 1
                                   and wc["Sample"]["Depth2"] == 1
                                   and wc["Sample"]["SampleDate"] in dates,
                        am.Query(key=key).map()):
        if value["Approved"]:
            value["Approved"] = False
            value["Remark"] = "OKA: Tas ut"
            print(am.putJson(token, am.aqua_site + f"/api/waterchemistry/{value['Id']}", value))

def insertValues(original, method_id, pairs):
    for value in am.Query(key=key).map():
        for pair in pairs:
            if value["Method"]["Name"] == original \
                              and value["Sample"]["SampleDate"] == pair["Date"]:
                if value["Approved"]:
                    value["Approved"] = False
                    value["Remark"] = "OKA: Erstattet med korrigert verdi"
                    print(am.putJson(token, am.aqua_site + f"/api/waterchemistry/{value['Id']}", value))
                    #print(value)
                new_value = {"Sample": {"Id": value["Sample"]["Id"]},
                             "Method": {"Id": method_id},
                             "Value": pair["Value"],
                             "Approved": True}
                print(am.postJson(token, am.aqua_site + "/api/waterchemistry", new_value))
                #print(new_value)

def iterateMethod(col, name):
    dates=[]
    for idx, row in table.iterrows():
        if not pd.isna(row[col]):
            dt = row[5]
            dates.append(f"{dt[6:10]}-{dt[3:5]}-{dt[:2]}T{dt[-8:]}Z")
    disapproveValues(name, dates)

def iterateMethodForInsert(col, original, method_id):
    pairs=[]
    for idx, row in table.iterrows():
        if not pd.isna(row[col]):
            pairs.append({"Date": row[1].strftime("%Y-%m-%dT%XZ"), "Value": round(row[col], 3)})
    insertValues(original, method_id, pairs)

year = "2016"
#station_name = "Lundevann"
station_id = 65381
#ph_korr_method_id = 35408
token = am.login()
key = openQuery(station_id, year)
#folder_name = "_Storelva-Nævestadfjorden\\_Lundevann\\Korreksjoner_ikke oppdatert i AM"

#table = pd.read_excel("K:\\Prosjekter\\Land-sea interactions\\_Loggedata fra LOI-stasjonene\\" +
#                      f"{folder_name}\\{station_name}_sensordata_{year}_inkl figs.xlsx",
#                      "Sensordata", header=1)
table = pd.read_excel("C:\\temp\\Verdier som kan fjernes_skjules i Aquamonitor.xlsx",
                      "Nævestadfjorden", header=1)

iterateMethod(9, "Temp")
#iterateMethod(6, "Kond")
#iterateMethod(8, "pH")
#iterateMethod(10, "Temp")
#iterateMethod(12, "Turb")
#iterateMethod(13, "CDOManalogFinal")
#iterateMethodForInsert(14, "pH", ph_korr_method_id)


#iterateMethod(11, "Vannstand")

#for v in am.Query(key=key).map():
#    print(v)