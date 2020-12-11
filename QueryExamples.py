import AquaMonitor

# Development
AquaMonitor.host = "http://localhost:52795/"
AquaMonitor.aqua_site = ""
AquaMonitor.cache_site = ""

#list = AquaMonitor.Query("project_id = 9566").list()
#print(list)
#gl_params = []

#stations = AquaMonitor.Query("project_id = 86 and sample_date > 01.01.1990 and sample_date <= 31.12.2016 and datatype=Plankton")\
#                      .map("Stations")
#for stat in stations:
#    print(stat)


#AquaMonitor.Query("project_id = 9566 and sample_date > 01.01.2016 and sample_date <= 31.12.2016")\
#            .makeArchive("excel", "Mjosa_2016.xlsx")\
#            .download("/Users/roar/Mjosa/")

chemistry = AquaMonitor.Query("project_id = 12433 and sample_date > 01.01.2019").map("water_chemistry_input")

for chem in chemistry:
    print(chem)
