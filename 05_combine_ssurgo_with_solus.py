
import os
import arcpy
import numpy as np
import pandas as pd
import seaborn as sns
import geopandas as gpd
import matplotlib.pyplot as plt
arcpy.env.overwriteOutput = True
"""
Combine SSURGO and SOLUS soil datasets.
Fill missing values in SSURGO using corresponding SOLUS variables.
Create plots
"""

# inputs
solus_file = r'B:\work_subset\projects\src\solus\outputs\v2\all_66k_values.xlsx'
root = os.path.dirname(os.path.abspath(__file__))
rating_all_states_file = os.path.join(root, 'outputs', 'v2', '04_rating_tables_all_variables',
                                       'ssurgo_ratings_all_variables_all_states.csv')
mapping_excel_file = os.path.join(root, 'soil_variables_mapping_between_ssurgo_solus.xlsx')

# input for checking missing data points only
POINTS_FC = r"B:\work_subset\projects\src\ssurgo\inputs\nri66k_points.gdb\nri66k_state_prj_ssurgo" 

# outputs
output_dir = os.path.join(root,'outputs', 'v2', '05_combine_ssurgo_solus')
output_excel_file_combined = os.path.join(output_dir, 'ssurgo_solus_combined.xlsx')
output_excel_file_before_combine = os.path.join(output_dir, 'ssurgo_before_combine.xlsx')
output_excel_file_missing_filling_count= os.path.join(output_dir, 'missing_filling_count.xlsx')

os.makedirs(output_dir, exist_ok=True)

def main():

    df_variable_mapping = pd.read_excel(mapping_excel_file, sheet_name='mapping')
    df_variable_mapping = df_variable_mapping[~df_variable_mapping.ssurgo.isna()]
    ssurgo_variables = df_variable_mapping.ssurgo.values


    if False:
        df_ssurgo = pd.read_csv(rating_all_states_file)
        df_solus = pd.read_excel(solus_file)
        df_ssurgo = df_ssurgo.set_index('PrimaryKey')
        df_solus = df_solus.set_index('PrimaryKey')

        df_ssurgo_selected = df_ssurgo[ssurgo_variables].copy()
        df_ssurgo_selected.reset_index().to_excel(output_excel_file_before_combine, index=False)

        df_missing_filling_count = pd.DataFrame([], columns=ssurgo_variables, index=['missing_before', 'missing_after'])
        for _, row in df_variable_mapping.iterrows():
            ssurgo_col = row['ssurgo']
            solus_col = row['solus']
            multiplier = row['multiplier']
            print(f"processing {ssurgo_col} {solus_col}...")

            if ssurgo_col in df_ssurgo.columns and solus_col in df_solus.columns:
                missing_before_filling = len(df_ssurgo_selected[df_ssurgo_selected[ssurgo_col].isna()])          
                solus_values_adjusted = df_solus[solus_col] * multiplier                
                df_ssurgo_selected[ssurgo_col] = df_ssurgo_selected[ssurgo_col].fillna(solus_values_adjusted)
                missing_after_filling = len(df_ssurgo_selected[df_ssurgo_selected[ssurgo_col].isna()])
                df_missing_filling_count[ssurgo_col]  = [missing_before_filling, missing_after_filling]
                

        df_ssurgo_selected.reset_index().to_excel(output_excel_file_combined, index=False)
        df_missing_filling_count.to_excel(output_excel_file_missing_filling_count)

    # plot
    df = pd.read_excel(output_excel_file_combined)
    # print_summary_stats(df)
    # plot_soil_distributions(df)

    # create fc
    create_missing_point_fc(df, POINTS_FC, ssurgo_variables)

    print(f"done")    


def create_missing_point_fc(df, point_fc, ssurgo_variables):
    gdb_path_source = os.path.dirname(point_fc)
    layer_name = os.path.basename(point_fc)
    gdf = gpd.read_file(gdb_path_source, layer=layer_name)

    # gdb_name = 'missing_datapoints.gdb'
    # missing_gdb = os.path.join(output_dir, gdb_name)

    # if not arcpy.Exists(missing_gdb):
    #     arcpy.management.CreateFileGDB(output_dir, gdb_name)

    gpkg_path = os.path.join(output_dir, 'missing_datapoints.gpkg')

    for var in ssurgo_variables:
        missing_keys = df.loc[df[var].isna(), 'PrimaryKey']
        missing_gdf = gdf[gdf['PrimaryKey'].isin(missing_keys)]        
        missing_gdf.to_file(gpkg_path, layer=f"{var}_missing", driver='GPKG')

        print(f"Exported missing points for {var}")


def plot_soil_distributions(df):
    numeric_df = df.select_dtypes(include=[np.number])
    sns.set_theme(style="whitegrid")
    cols = numeric_df.columns
    num_cols = len(cols)
    
    if num_cols == 0:
        print("No numeric columns found for distribution plots.")
        return
    
    fig, axes = plt.subplots(nrows=num_cols, ncols=2, figsize=(12, 4 * num_cols))
    
    if num_cols == 1:
        axes = axes.reshape(1, -1)
    
    fig.subplots_adjust(hspace=0.4)
    
    for i, col in enumerate(cols):
        # --- Histogram + KDE (Distribution & Shape) ---
        sns.histplot(numeric_df[col].dropna(), kde=True, ax=axes[i, 0], color='skyblue')
        axes[i, 0].set_title(f'Distribution of {col}')
        axes[i, 0].set_xlabel(col)
        
        # --- Boxplot (Outlier Detection) ---
        sns.boxplot(x=numeric_df[col].dropna(), ax=axes[i, 1], color='lightcoral')
        axes[i, 1].set_title(f'Outliers in {col}')
        axes[i, 1].set_xlabel(col)

        # Add missing data annotation
        missing_count = df[col].isnull().sum()
        missing_pct = (missing_count / len(df)) * 100
        axes[i, 0].text(0.95, 0.95, f'Missing: {missing_count} ({missing_pct:.1f}%)', 
                        transform=axes[i, 0].transAxes, ha='right', va='top', 
                        bbox=dict(boxstyle='round', facecolor='white', alpha=0.5))

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'soil_distributions.png'), dpi=150, bbox_inches='tight')
    print(f"Saved distribution plots to {os.path.join(output_dir, 'soil_distributions.png')}")
    plt.close()


def print_summary_stats(df):
    """Print summary statistics for the dataset"""
    print("\n" + "="*60)
    print("DATASET SUMMARY")
    print("="*60)
    print(f"Total rows: {len(df)}")
    print(f"Total columns: {len(df.columns)}")
    
    print("\n--- Missing Data Summary ---")
    missing = df.isnull().sum()
    missing_pct = (missing / len(df)) * 100
    missing_df = pd.DataFrame({'Missing Count': missing, 'Missing %': missing_pct})
    missing_df = missing_df[missing_df['Missing Count'] > 0].sort_values('Missing %', ascending=False)
    if len(missing_df) > 0:
        print(missing_df.to_string())
    else:
        print("No missing data found.")
    
    df.describe().to_csv(os.path.join(output_dir, 'combined_statistics.csv'))

if __name__ == "__main__":
    main()