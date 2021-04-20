__author__ = 'Roar Brenden'

import aquamonitor as am
#am.host = "https://test-aquamonitor.niva.no/"
token = am.login()
site = "mjosovervak"
where = "project_id=1098 and sample_date>01.01.2018 and sample_date<=31.12.2018 and Plankton.parameter_id=2"
width = 650
height = 300
root = "C:/temp/"

for stationId in am.Query(
        where=where). \
        map():
    file = root + "plankton_" + str(stationId) + ".png"
    am.Graph(width, height, token=token, site=site, graph='/Graph/Stolpediagram.ashx', stationId=stationId,
             parameter='Plankton;2',
             where=where)\
        .download(file)
