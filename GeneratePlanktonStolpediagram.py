__author__ = 'Roar Brenden'


import AquaMonitor

token = AquaMonitor.login()

site = "mjosovervak"

width = 650
height = 300
year = 2015
stationId = 31342
path = "/Graph/Stolpediagram.ashx?w=" + str(width) + "&h=" + str(height) + "&stid=" + str(stationId) + "&p=Plankton;2&ses=" + str(year)

root = "/users/roar/"
resp = AquaMonitor.get(token, site, path)
file = root + "plankton_" + str(stationId) + ".png"

AquaMonitor.downloadFile(token, site + path, file)