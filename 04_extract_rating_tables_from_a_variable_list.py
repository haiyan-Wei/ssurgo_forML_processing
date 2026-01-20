import os
import time
import arcpy
import sqlite3
import numpy as np
import pandas as pd
from pathlib import Path
from collections import defaultdict
t0 = time.time()
arcpy.env.overwriteOutput = True

''' this script is used to extract all rating tables from the SSURGO database into a new gdb.
input data: 
1. nri data points with projection and ssurgo gpkg for each state
2. SSURGO database for each state
3. a list of rating tables to extract (from 02_get_rating_tables_summary.py)    
output: 
1. gdb with all rating tables for each state
2. csv file with all rating tables for all states 
Note this takes a while to run.

process:
1. spatial join nri data points with ssurgo gpkg for each state
2. extract all rating tables from the SSURGO database for each state
3. merge all rating tables for all states into output_csv_path
'''

# inputs
PRIMARY_KEY_FIELD = "PrimaryKey"
POINTS_FC = r"B:\work_subset\projects\src\ssurgo\inputs\nri66k_points.gdb\nri66k_state_prj_ssurgo" 
STATE_GPKG_DIR = r"B:\work_subset\projects\data\ssurgo_portal\02_gpkg_by_state_database"

ROOT = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(ROOT, "outputs", 'v2', '04_rating_tables_all_variables')
output_csv_path = os.path.join(OUTPUT_DIR, 'ssurgo_ratings_all_variables_all_states.csv')
output_summary_excel = os.path.join(OUTPUT_DIR, 'ssurgo_ratings_all_variables_all_states_summary.xlsx')
output_missing_primarykeys_fc = os.path.join(OUTPUT_DIR, 'ssurgo_missing_primarykeys.gdb', 'ssurgo_missing_primarykeys')

output_temp_state_csv_dir = os.path.join(OUTPUT_DIR, 'ssurgo_csv_by_state')
output_temp_gdb_path = os.path.join(OUTPUT_DIR, 'temp_extract_ssurgo.gdb')


def main():
    create_output_directories()
    RATING_TABLES = ['main.rating_BdSurf_WA_0_5_cm',    
        'main.rating_BdSurf_WA_25_30_cm',
        'main.rating_BdSurf_WA_SL',
        'main.rating_CEC7_WA_0_5_cm',
        'main.rating_CEC7_WA_25_30_cm',
        'main.rating_CEC7_WA_SL',
        'main.rating_Clay_WA_0_5_cm',
        'main.rating_Clay_WA_25_30_cm',
        'main.rating_Clay_WA_SL',
        'main.rating_Dep2AnyRes_WA',
        'main.rating_Dep2BedRS_WA',
        'main.rating_Dep2WatTbl_WA_jan_dec',
        'main.rating_EcoSiteID_DCD',
        'main.rating_EcoSiteNm_DCD',
        'main.rating_Sand_WA_0_5_cm',
        'main.rating_Sand_WA_25_30_cm',
        'main.rating_Sand_WA_SL',
        'main.rating_omr_WA_0_5_cm',
        'main.rating_omr_WA_25_30_cm',          
        'main.rating_omr_WA_SL',
        'main.rating_pHSurf_WA_0_5_cm',
        'main.rating_pHSurf_WA_25_30_cm',   
        'main.rating_pHSurf_WA_SL',
        ]

    print(f"Number of rating tables to extract: {len(RATING_TABLES)}")   
    
    if False: 

        state_list = get_states_from_gpkglist(STATE_GPKG_DIR)

        for i, state in enumerate(state_list):
            print(f'processing {state} {i+1}/{len(state_list)}')

            sjoin_output = os.path.join(output_temp_gdb_path, f'{state}_sj')
            state_rating_results_csv  = os.path.join(output_temp_state_csv_dir, f'{state}_rating.csv')
            os.makedirs(os.path.join(OUTPUT_DIR, 'state_csv'), exist_ok=True)

            if not os.path.exists(state_rating_results_csv):
                gpkg_path, soil_poly =  get_state_gpkg_path(state, STATE_GPKG_DIR)
                spatial_join(POINTS_FC, soil_poly, output_temp_gdb_path, sjoin_output)
                state_df = read_ratings(RATING_TABLES, gpkg_path, state, sjoin_output, state_rating_results_csv)         
    
        # merge
        rating_all_states = pd.DataFrame()
        for i, state in enumerate(state_list):
            state_csv  = os.path.join(output_temp_state_csv_dir, f'{state}_rating.csv')
            if not os.path.exists(state_csv):            
                raise FileNotFoundError(f"Missing CSV file for state {state}: {state_csv}")
            else:    
                state_df = pd.read_csv(state_csv)
                rating_all_states = pd.concat([state_df, rating_all_states], ignore_index=True)

        rating_all_states.to_csv(output_csv_path, index=False)

    summarize_rating_data(output_csv_path, POINTS_FC, output_summary_excel)
    
    t1 = time.time()
    runtime = (t1 - t0) / 60.    
    print(f"\n Done. Time taken: {runtime:.2f} minutes")
    

def summarize_rating_data(csv_path, points_fc, output_summary_excel):
    # two outputs:
    # 1. summary excel file
    # 2. feature class for missing primary keys
    
    # get total primary keys from points fc and missing keys
    df = pd.read_csv(csv_path)
    data = []
    with arcpy.da.SearchCursor(points_fc, [PRIMARY_KEY_FIELD]) as cursor:
        for row in cursor:
            data.append(list(row))
    total_primarykeys_df = pd.DataFrame(data, columns=[PRIMARY_KEY_FIELD])

    missing_primarykeys = total_primarykeys_df[~total_primarykeys_df.PrimaryKey.isin(df.PrimaryKey)]
    print(f'\nnumber of PrimaryKey in points fc: {len(total_primarykeys_df)}, unique number of PrimaryKey: {len(total_primarykeys_df.PrimaryKey.unique())}')
    print(f'number of PrimaryKey in final table: {len(df)}, unique number of PrimaryKey: {len(df.PrimaryKey.unique())}')
    print(f'number of primary keys in points fc but not in final table: {len(missing_primarykeys)}')

    missing_primarykeys.to_csv(os.path.join(OUTPUT_DIR, 'missing_primarykeys.csv'), index=True)    
    gdb_parent = os.path.dirname(points_fc)
    gdb_name = os.path.basename(points_fc)
    gdf = gpd.read_file(gdb_parent, layer=gdb_name)
    missing_gdf = gdf[~gdf[PRIMARY_KEY_FIELD].isin(df[PRIMARY_KEY_FIELD])]
    missing_gdf.to_file(output_missing_primarykeys_fc, driver='GPKG')

    # get missing variables
    variables = [col for col in df.columns if col not in ['PrimaryKey', 'mukey', 'state']]   
    missing_vars = df[variables].isnull().sum().to_frame(name='Missing_Count')
    missing_vars['Missing_Percent'] = (missing_vars['Missing_Count'] / len(df) * 100).round(2)
    missing_vars = missing_vars.sort_values(by='Missing_Count', ascending=False)

    # get state summary
    state_counts = df['state'].value_counts().to_frame(name='Total_Points_Found')
    state_missing_count = df.groupby('state')[variables].apply(lambda x: x.isnull().sum())
    state_summary = state_counts.join(state_missing_count)
    # Add total missing count across all variables per state
    state_summary['Total_Missing'] = state_summary[variables].sum(axis=1)
    state_summary = state_summary.sort_values(by='Total_Missing', ascending=False)

    # write to excel
    with pd.ExcelWriter(output_summary_excel) as writer:
        missing_vars.to_excel(writer, sheet_name='Missing_Variables')
        state_summary.to_excel(writer, sheet_name='State_Summary')
        missing_primarykeys.to_excel(writer, sheet_name='Missing_PrimaryKeys')



def read_ratings(RATING_TABLES, gpkg_path, state, sjoin_output, rating_results_csv):

    
    data = []
    with arcpy.da.SearchCursor(sjoin_output, [PRIMARY_KEY_FIELD, 'mukey']) as cursor:
        for row in cursor:
            data.append(list(row))
    sp_join_df = pd.DataFrame(data, columns=[PRIMARY_KEY_FIELD, 'mukey'])
        
    for i, table_name in enumerate(RATING_TABLES):        
        current_df = read_gpkg_table_to_df(gpkg_path, table_name)            
        current_df['mukey'] = pd.to_numeric(current_df['mukey'], errors='coerce')
        current_df = current_df[['mukey', table_name.replace('main.rating_', '')]]
        
        if i==0:
            base_df = current_df
        else:
            base_df = pd.merge(base_df, current_df, on='mukey', how='outer')
    
    state_result_df = pd.merge(sp_join_df, base_df, on='mukey', how='left')
    state_result_df['state'] = state
    state_result_df = state_result_df.dropna(subset=['mukey'])
    state_result_df.to_csv(rating_results_csv, index=False)

    return state_result_df    
    

def get_states_from_gpkglist(STATE_GPKG_DIR):
    gpkglist = os.listdir(STATE_GPKG_DIR)
    states = [foldername.replace('_gpkg', '') for foldername in gpkglist]
    return sorted(states)

def spatial_join(point_fc, soil_poly, output_gdb_path, sjoin_output):

    arcpy.analysis.SpatialJoin(
        target_features=point_fc,
        join_features=soil_poly,
        out_feature_class=sjoin_output,
        join_operation="JOIN_ONE_TO_ONE",
        join_type="KEEP_ALL",
        match_option="INTERSECT",
        search_radius=None,
        distance_field_name="",
        match_fields=None)


def get_state_gpkg_path(state_name, gpkg_dir):
    gpkg_path = os.path.join(gpkg_dir, f"{state_name}_gpkg", f"{state_name}.gpkg")
    if not os.path.exists(gpkg_path):
        raise FileNotFoundError(f"GPKG not found for {state_name}")
    
    soil_poly = os.path.join(gpkg_path, "MUPOLYGON")
    if not arcpy.Exists(soil_poly):
        raise FileNotFoundError(f"'MUPOLYGON' not found in GPKG for {state_name}")
    
    return gpkg_path, soil_poly


def get_states(points_fc):
    states = set()
    with arcpy.da.SearchCursor(points_fc, ["STATE_NAME"]) as cursor:
        for row in cursor:
            states.add(row[0])
    state_list = sorted(list(states))
    return state_list


def load_rating_availability(csv_path):    
    df = pd.read_csv(csv_path)
    
    availability = {}
    for idx, row in df.iterrows():
        rating_table = row['rating_tables']
        state_availability = {}
        
        for col in df.columns[1:]:  
            
            has_rating = str(row[col]).strip().lower() == 'yes'
            state_availability[col] = has_rating
        
        availability[rating_table] = state_availability
    
    rating_tables_from_csv = df['rating_tables'].tolist()
        
    return availability, rating_tables_from_csv


def read_gpkg_table_to_df(gpkg_path, table_name):
    try:
        conn = sqlite3.connect(gpkg_path)
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        conn.close()
        return df
    except Exception as e:
        print(f"   WARNING: Could not read table '{table_name}' with sqlite3. {e}")
        return None


def ensure_gdb_exists(gdb_path):
    """Create a file geodatabase if it doesn't exist."""
    if not arcpy.Exists(gdb_path):
        gdb_parent = os.path.dirname(gdb_path)
        gdb_name = os.path.basename(gdb_path)
        arcpy.management.CreateFileGDB(gdb_parent, gdb_name)

def create_output_directories():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    if not os.path.exists(output_temp_state_csv_dir):
        os.makedirs(output_temp_state_csv_dir)
    
    ensure_gdb_exists(output_temp_gdb_path)
    ensure_gdb_exists(os.path.dirname(output_missing_primarykeys_fc))


if __name__ == "__main__":
    main()