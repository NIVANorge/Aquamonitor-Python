# Aquamonitor-Python

A Python library providing external access to Nivabasen, NIVA's internal water chemistry and ecology database.

## Installation

Aquamonitor-Python is pre-installed on [NIVA's JupyterHub](https://jupyterhub.niva.no). To install on a local machine

    python -m pip install --no-cache-dir git+https://github.com/NIVANorge/Aquamonitor-Python.git
    
## Quick start

``` python
import aquamonitor as am

# Get access token.
# Username/password could be placed in a file ".auth", or you will get a Login window.
token = am.login()

# List all projects
proj_df = am.get_projects(token=token)
print(f"{len(proj_df)} projects in the database.")
print(proj_df.head())

# Get stations in project
proj_id = 12433
stn_df = am.get_project_stations(proj_id, token=token)
print(f"{len(stn_df)} stations in project.")
print(stn_df.head())

# Get water chemistry for project and time period
proj_id = 12433
st_dt = "01.01.2019"
end_dt = "31.12.2019"

df = am.get_project_chemistry(proj_id, st_dt, end_dt, token=token, approved=True)
print(df.head())
```
## Authorisation
AquaMonitor API is using the same users and privileges as AquaMonitor. Meaning that as an internal NIVA user you can use your own username / password and get access to all stations.

External users would also have access, but for a limited set of stations.

See also the `examples` folder and the Jupyter notebook [here](https://nbviewer.org/github/NIVANorge/Aquamonitor-Python/blob/master/examples/query_chem.ipynb).
