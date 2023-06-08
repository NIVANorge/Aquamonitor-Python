import aquamonitor as am
import pandas as pd

#am.host = "https://test-aquamonitor.niva.no/"

def openQuery(station_id, year):
    """
    Create a new query for water chemistry input values for the station in a specific year.
    :param station_id: Nivadatabase station_id
    :param year: The year
    :return: The key to use in later requests
    """
    query = am.Query(where=f"sample_date>=01.01.{year} and sample_date<=31.12.{year}",
                     stations=[station_id],
                     table="water_chemistry_input")
    query.createQuery()
    return query.key


def printQuery(key):
    """
    Fetch the result of the given query and print each item.
    :param key: The key to the result
    :return: None
    """
    am.Query(key=key).map(print)


def filterMethod(key, name):
    """
    Filter the result by a specific method.
    :param key: The key to the result
    :param name: The name of the method
    :return: The items with the specific method
    """
    return filter(lambda c: c["Method"]["Name"] == name, am.Query(key=key).map())


def approveValues(name, dates):
    """
    Set the value with method at the given date to Approved. At the same time erase whatever is in Remark.
    Output's the resulting value item.
    :param name: Name of the method
    :param dates: Array of sample dates
    :return: None
    """
    for value in filter(lambda wc: wc["Method"]["Name"] == name and wc["Sample"]["SampleDate"] in dates,
                        am.Query(key=key).list()):
        value["Approved"] = True
        value["Remark"] = ""
        am.putJson(token, am.aqua_site + f"/api/waterchemistry/{value['Id']}", value)
        print(value)

def disapproveValues(name, dates, depth):
    """
    Set Approved = false for each value with a specific method sampled at different dates.
    :param name: Name of the method
    :param dates: Array of dates
    :param depth: Depth to filter on
    :return:
    """
    for value in filter(lambda wc: wc["Method"].get("Name") == name
                                   and wc["Sample"]["SampleDate"] in dates
                                   and wc["Sample"]["Depth1"] == depth,
                        am.Query(key=key).list()):
        if value["Approved"]:
            value["Approved"] = False
            value["Remark"] = "OKA: Tas ut"
            print(am.putJson(token, am.aqua_site + f"/api/waterchemistry/{value['Id']}", value))

def insertValues(original, method_id, pairs):
    """
    Replaces a value with a new item having a different method. At the same time disapprove the original.
    :param original: Name of method to disapprove
    :param method_id: New method id
    :param pairs: Array of date/value pairs
    :return:
    """
    for value in am.Query(key=key).list():
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

def iterateMethodForDisapprovement(col, name, depth):
    """
    Loop through the Excel sheet at a specific column to create array of dates where there are content in the field.
    The date should be in a specific column.
    :param col:
    :param name:
    :return:
    """
    date_col = 1
    depth_col = 2
    dates=[]
    for idx, row in table.iterrows():
        if row[depth_col] == depth and not pd.isna(row[col]):
            dt = row[date_col]
            #dates.append(f"{dt[6:10]}-{dt[3:5]}-{dt[:2]}T{dt[-8:]}Z") # Given as a String
            dates.append(dt.strftime("%Y-%m-%dT%XZ")) # Given as Timestamp
    disapproveValues(name, dates, depth)

def iterateMethodForInsert(col, original, method_id):
    pairs=[]
    date_col = 1
    for idx, row in table.iterrows():
        if not pd.isna(row[col]):
            pairs.append({"Date": row[date_col].strftime("%Y-%m-%dT%XZ"), "Value": round(row[col], 3)})
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

#iterateMethod(9, "Temp")
#iterateMethod(6, "Kond")
#iterateMethod(8, "pH")
#iterateMethod(10, "Temp")
#iterateMethod(12, "Turb")
#iterateMethod(13, "CDOManalogFinal")
#iterateMethodForInsert(14, "pH", ph_korr_method_id)


#iterateMethod(11, "Vannstand")

#for v in am.Query(key=key).map():
#    print(v)