import aquamonitor as am

# Development
#AquaMonitor.host = "http://localhost:52795/"
#AquaMonitor.aqua_site = ""
#AquaMonitor.cache_site = ""

# Test
#am.host = "https://test-aquamonitor.niva.no/"

#list = am.Query("project_id = 9566").list()
#print(list)
#gl_params = []

# stations = am.Query("project_id = 86 and sample_date > 01.01.1990 and sample_date <= 31.12.2016 and datatype=Plankton")\
#                       .map("Stations")
# for stat in stations:
#     print(stat)


#am.Query("project_id = 9566 and sample_date > 01.01.2016 and sample_date <= 31.12.2016")\
#            .makeArchive("excel", "Mjosa_2016.xlsx")\
#            .download("/Users/roar/Mjosa/")

pages = am.Query("project_id = 12433 and sample_date > 01.01.2019 and sample_date <= 31.12.2019",
                 table="water_chemistry_output")\
                        .pages()

print(pages.total)
#
# This is the result of the call for query/{key}/water_chemistry_output
# The resultset can be obtained by iterating over all the pages given by chemistry.Pages
# The path for a page would be query/{key}/water_chemistry_output/{page}
# Page is 0-based
# The key is hidden within AquaMonitor.Query.key

tenth = pages.fetch(9)
print(tenth)
