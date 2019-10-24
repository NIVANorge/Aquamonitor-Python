import AquaMonitor

AquaMonitor.host = "http://localhost:65493/"
AquaMonitor.aqua_site = ""

token = AquaMonitor.login()
id = "10910818"

sample = AquaMonitor.getJson(token, "api/data/watersamples/" + id)
print(sample)

list = AquaMonitor.getJson(token, "api/labware/links?table=water&sampleId=" + id)
print(list)

for i in list:
    lims = AquaMonitor.getJson(token, "api/labware/samples/" + str(i["SampleNumber"]))
    print(lims)
