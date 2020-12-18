# Aquamonitor-Python

A Python library providing external access to Nivabasen, NIVA's internal water chemistry database.

## Installation

Aquamonitor-Python is pre-installed on NIVA's JupyterHub - see **Tutorial 04** in the `dstoolkit_examples/tutorials` folder for suggestions on how to get started.

Otherwise, to install on a local machine

    python -m pip install --no-cache-dir git+https://github.com/NIVANorge/Aquamonitor-Python.git@chem_queries
    
## Quick start

``` python
import aquamonitor as am
am.host = "https://test-aquamonitor.niva.no/"

# Get access token
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

See also the `examples` folder.