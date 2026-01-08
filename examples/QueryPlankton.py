import aquamonitor as am
token = am.login()
qry = am.Query(token = token, stations = [10201], table = "plankton_parameters", where = "Plankton.parameter_id=7")
res = qry.list()
print(res)