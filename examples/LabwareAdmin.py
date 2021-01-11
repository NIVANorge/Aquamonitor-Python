import aquamonitor as am


#am.host = "http://localhost:65493/"
#am.aqua_site = ""

am.host = "https://test-aquamonitor.niva.no/"
am.aqua_site = "admin/"

token = am.login()
id = "10910818"

sample = am.getJson(token, am.aqua_site + "api/data/watersamples/" + id)
print(sample)

list = am.getJson(token, am.aqua_site + "lab/api/links?table=water&sampleId=" + id)
print(list)

for i in list:
    lims = am.getJson(token, am.aqua_site + "lab/api/samples/" + str(i["SampleNumber"]))
    print(lims)

