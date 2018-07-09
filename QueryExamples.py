import AquaMonitor

AquaMonitor.host = "http://151.157.129.195/"

#list = AquaMonitor.Query("project_id = 9566").list()
#print(list)
gl_params = []

params = AquaMonitor.Query("project_id = 86 and sample_date > 01.01.1990 and sample_date <= 31.12.2016 and parameter_id = Water;261 or parameter_id=Water;11 or parameter_id=Water").list("Parameter")
for param in params:

    print("ID:" + str(param["Id"]) + " - " + param["Name"])


#AquaMonitor.Query("project_id = 9566 and sample_date > 01.01.2016 and sample_date <= 31.12.2016")\
#            .makeArchive("excel", "Mjosa_2016.xlsx")\
#            .download("/Users/roar/Mjosa/")