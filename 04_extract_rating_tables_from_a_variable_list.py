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

ROOT = os.path.dirname(os.path.abspath(__file__))
POINTS_FC = r"D:\projects\src\ssurgo\inputs\nri66k_points.gdb\nri66k_state_prj_ssurgo"
STATE_GPKG_DIR = r"D:\projects\data\ssurgo\Databases"

OUTPUT_DIR = os.path.join(ROOT, "output_sandclay04")

STATE_FIELD = "STATE_NAME"
PRIMARY_KEY_FIELD = "PrimaryKey"

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)
output_gdb_path = os.path.join(OUTPUT_DIR, 'ssurgo_extract.gdb')
if not arcpy.Exists(output_gdb_path):
    arcpy.management.CreateFileGDB(OUTPUT_DIR, 'ssurgo_extract.gdb')
arcpy.env.workspace = output_gdb_path
arcpy.env.overwriteOutput = True


def main():
    
    # availability, rating_tables_from_csv = load_rating_availability(RATING_SUMMARY_CSV)    
    # RATING_TABLES = rating_tables_from_csv
    # if True:
    #     RATING_TABLES = ['main.rating_Dep2WatTbl_DCD_jan_dec','main.rating_FloodFCls_DCD_jan_dec',
    #                      'main.rating_PondFCls_DCD_jan_dec','main.rating_GravelSrc_DCD']    


    # RATING_TABLES = ['main.rating_BdSurf_DCD_SL', 'main.rating_Dep2BedRS_WA', 'main.rating_Dep2AnyRes_WA', 'main.rating_Dep2WatTbl_WA_jan_dec']
    RATING_TABLES = ['main.rating_Clay_WA_0_4_cm', 'main.rating_Sand_WA_0_4_cm', 'main.rating_Silt_WA_0_4_cm']

    print(f"Number of rating tables to extract: {len(RATING_TABLES)}")    
    
    state_list = get_states_from_gpkglist(STATE_GPKG_DIR)  #alabama

    # read table
    if True: 
        for i, state in enumerate(state_list):
            print(f'processing {state} {i+1}/{len(state_list)}')

            sjoin_output = os.path.join(output_gdb_path, f'{state}_sj')
            state_rating_results_csv  = os.path.join(OUTPUT_DIR, 'state_csv', f'{state}_rating.csv')
            os.makedirs(os.path.join(OUTPUT_DIR, 'state_csv'), exist_ok=True)

            if not os.path.exists(state_rating_results_csv):
                gpkg_path, soil_poly =  get_state_gpkg_path(state, STATE_GPKG_DIR)
                spatial_join(POINTS_FC, soil_poly, output_gdb_path, sjoin_output)
                state_df = read_ratings(RATING_TABLES, gpkg_path, state, sjoin_output, state_rating_results_csv)   
        
    
    # merge
    rating_all_states = pd.DataFrame()
    for i, state in enumerate(state_list):
        state_csv  = os.path.join(OUTPUT_DIR, 'state_csv', f'{state}_rating.csv')
        if not os.path.exists(state_csv):            
            raise FileNotFoundError(f"Missing CSV file for state {state}: {state_csv}")
        else:    
            state_df = pd.read_csv(state_csv)
            rating_all_states = pd.concat([state_df, rating_all_states], ignore_index=True)

    rating_all_states.to_csv(os.path.join(OUTPUT_DIR, 'state_csv','all_states.csv'), index=False)

    summarize_rating_data(rating_all_states)

    
    t1 = time.time()
    runtime = (t1 - t0) / 60.
    
    print(f"\n Done. Time taken: {runtime:.2f} minutes")

def summarize_rating_data(rating_all_states):
    print(f'number of PrimaryKey in final table: {len(rating_all_states)}, {len(rating_all_states.PrimaryKey.unique())}')



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

if __name__ == "__main__":
    main()