import AquaMonitor
import requests

#AquaMonitor.host = "http://localhost:65493/"
#AquaMonitor.aqua_site = ""

AquaMonitor.host = "https://test-aquamonitor.niva.no/"
AquaMonitor.aqua_site = "admin/"


token = AquaMonitor.login()


def queryGraph(token, document):
    resp = requests.post(AquaMonitor.host + AquaMonitor.aqua_site + "lab/graphql", json=document,
                         cookies=dict(aqua_key=token))
    print(resp.text)


print("Get all projects within 1000-lakes")
queryGraph(token, {
    "query": "query getProjects($nr: String) {projects(projectNr: $nr){name}}",
    "variables": {
        "nr": "190091;3"
    }
})


print("Get all samples within a single project.")
queryGraph(token, {
    "query": "query getSamples($name: String) {samples(projectName: $name){sampleNumber,textID,nivadatabaseStation{id,code,name,type{text}},status,sampledDate}}",
    "variables": {
        "name": "930-7875"
    }
})


print("Get the results from one sample.")
queryGraph(token, {
    "query": "query getResults($nr: Int) {results(sampleNumber: $nr){name,units,analysis,accreditedId,entryQualifier,"
             "numericEntry,mu,loq,status}}",
    "variables": {
        "nr": 102689
    }
})


print("Check status of Feltfo.")
queryGraph(token, {
    "query": "query getProjects($nr: String) {projects(projectNr: $nr){name,samples{sampleNumber,status,nivadatabaseStation{code},sampledDate}}}",
    "variables": {
        "nr": "17127;1:FELT"
    }
})
