import AquaMonitor


#AquaMonitor.host = "http://localhost:65493/"
#AquaMonitor.aqua_site = ""

AquaMonitor.host = "https://test-aquamonitor.niva.no/"
AquaMonitor.aqua_site = "admin/"

token = AquaMonitor.login()
id = "10910818"

sample = AquaMonitor.getJson(token, AquaMonitor.aqua_site + "api/data/watersamples/" + id)
print(sample)

list = AquaMonitor.getJson(token, AquaMonitor.aqua_site + "lab/api/links?table=water&sampleId=" + id)
print(list)

for i in list:
    lims = AquaMonitor.getJson(token, AquaMonitor.aqua_site + "lab/api/samples/" + str(i["SampleNumber"]))
    print(lims)

