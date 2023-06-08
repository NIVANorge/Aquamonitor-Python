import aquamonitor.aquamonitor as am

#am.host = "https://test-aquamonitor.niva.no/"

token = am.login()

root = "/aquaservices/api/projects/12450/stations/73213/biota/"

samples = am.getJson(token, root + "samples")
for sample in samples:
    print(sample)


values = am.getJson(token, root + "samples/244995/values")
for value in values:
    print(value)

specimens = am.getJson(token, root + "samples/244995/specimens")
for specimen in specimens:
    print(specimen)


specimens = am.getJson(token, root + "specimens")
for specimen in specimens:
    print(specimen)

