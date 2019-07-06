import AquaMonitor as am


am.host = "https://test-aquamonitor.niva.no/"

# PTI -> plankton.parameter_id = 7

am.Query(where="Plankton.parameter_id=7")\
   .makeArchive(fileformat="excel", filename="Plankton.xlsx")\
   .download(path="C:/Naturindeks/")

# PIT -> begroing.parameter_id = 1
# AIP -> begroing.parameter_id = 2

am.Query(where="Begroing.parameter_id in (1,2)")\
    .makeArchive(fileformat="excel", filename="Begroing.xlsx")\
    .download(path="C:/Naturindeks/")