### Goal is to retrive all TOTP taken in Glomma vannområde, and have it as a single call.
### The extent of Glomma vannområde will be taken from Geoserver at this url:
# https://aquamonitor.niva.no/geoserver/web

import aquamonitor as am

token = am.login()

## To get the parameter_id for TOTP, we use the function:
am.get_water_parameters("TOTP", token)

where = "Water.parameter_id=11"

## We define the geography from looking into Geoserver.
geography = {
    "layer": "no.niva:nve_vassdragomr_f",
    "fieldName": "vassomrnr",
    "textValue": "002"
}

df = am.Query(where=where, 
         geography=geography, 
         token=token, 
         table="water_chemistry_output").getDataFrame()