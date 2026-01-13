import arcpy
import os
import pandas as pd

""" use this script to get a summary table of all rating tables in all states, so if you a missing variable, go run portal again and get it"""


# input
database_path = r"D:\work\data\ssurgo_download\DATABSE20251213"
output_tablename = "rating_tables_20260108.csv"

# output
workspace = os.path.dirname(os.path.abspath(__file__))
output_directory = os.path.join(workspace, "output")

df_rating_tables = pd.DataFrame([])
original_workspace = arcpy.env.workspace

for folder in os.listdir(database_path):
    state = folder.replace("_gpkg", "")
    print(f"Processing {state}")
    gpkg = os.path.join(database_path, folder, f"{state}.gpkg")
    arcpy.env.workspace = gpkg
    all_tables = arcpy.ListTables()     
    rating_tables = [t for t in all_tables if t.startswith("main.rating_")]
    if len(rating_tables) == 0:
        print(f"No rating tables found for {state}")
        continue
    df_rating_table = pd.DataFrame({f"{state}": 'yes', "rating_tables": rating_tables})
    df_rating_table = df_rating_table.set_index("rating_tables")
    df_rating_tables = pd.merge(df_rating_tables, df_rating_table, left_index=True, right_index=True, how="outer")    
    arcpy.env.workspace = original_workspace

df_rating_tables.to_csv(os.path.join(output_directory, output_tablename), index=True)
print(f"results see {output_tablename}")

# print(df_rating_tables)

# for state in df_rating_tables.columns:
#     # print missing rating tables
#     missing_rating_tables = df_rating_tables[df_rating_tables[state] != 'yes'].index.tolist()
#     print(f"\n\n{state}\n{', '.join(missing_rating_tables)}")

# print("Done!")

# print rating tables for one state
# for state in ['Pennsylvania']:
#     print(f"Processing {state}")
#     gpkg = os.path.join(database_path, state + '_gpkg', f"{state}.gpkg")
#     arcpy.env.workspace = gpkg
#     all_tables = arcpy.ListTables()
#     rating_tables = [t for t in all_tables if t.startswith("main.rating_")]
#     for table in rating_tables:
#         print(table)

