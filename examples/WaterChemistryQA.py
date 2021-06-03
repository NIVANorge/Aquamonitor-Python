import aquamonitor as am

am.host = "https://test-aquamonitor.niva.no/"


def writeKonduktivitet(wc):
    if wc.get("Parameter").get("Name") == "Konduktivitet":
        print(wc.get("Sample").get("SampleDate") + " " + str(wc.get("Value")))


am.Query(where="sample_date>=01.01.2018 and sample_date<=31.12.2018", stations=[69706],
         table="water_chemistry_output")\
    .map(writeKonduktivitet)



