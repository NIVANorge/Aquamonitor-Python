__author__ = 'Roar Brenden'

import aquamonitor.aquamonitor as am
token = am.login()
site = "mjosovervak"
where = "project_id=1098 and sample_date>01.01.2020 and sample_date<=31.12.2020 and Plankton.parameter_id=2"
width = 650
height = 300
root = "C:/temp/"

for station_id in am.Query(where=where).list():
    file = root + "plankton_" + str(station_id) + ".png"
    am.Graph(width, height, token=token, site=site, graph='/Graph/Stolpediagram.ashx', stationId=station_id,
             parameter='Plankton;2', where=where)\
        .download(file)
