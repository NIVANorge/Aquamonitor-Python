import aquamonitor as am
import pandas as pd

token = am.login()

# Load full input table
df = am.Query(where="project_id=10826 and sample_date>01.01.2009 and sample_date<01.01.2011", 
              table="water_chemistry_input",
              token=token).getDataFrame()

# Filter for pH values
df = df[df["Method.Name"].str.startswith("pH")]

# Extract necessary columns
print(df.columns)
df = df[["Method.Name", "Value", "Sample.Depth1", "Sample.Depth2",
         "Sample.Station.Project.Name", "Sample.Station.Name", "Sample.Station.Code",
         "Sample.SampleDate"]].copy()

# Add a column with year
df["Year"] = pd.to_datetime(df["Sample.SampleDate"]).dt.year
