import AquaMonitor

AquaMonitor.host = "http://151.157.129.195/"

#list = AquaMonitor.Query("project_id = 9566").list()
#print(list)
gl_params = []

stations = AquaMonitor.Query("project_id = 86 and sample_date > 01.01.1990 and sample_date <= 31.12.2016 and datatype=Plankton").map("Stations")
for stat in stations:
    print(stat)


#AquaMonitor.Query("project_id = 9566 and sample_date > 01.01.2016 and sample_date <= 31.12.2016")\
#            .makeArchive("excel", "Mjosa_2016.xlsx")\
#            .download("/Users/roar/Mjosa/")