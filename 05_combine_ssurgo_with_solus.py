import os
import pandas as pd

ROOT = os.path.dirname(os.path.abspath(__file__))
rating_all_states = os.path.join(ROOT, 'output_bd_v2', 'state_csv','all_states.csv')

solus_file = r'D:\projects\src\solus\output\all_66k_values.xlsx'

df_ratings_allstates = pd.read_csv(rating_all_states)
df_solus = pd.read_excel(solus_file)

soil_variables = [c for c in list(df_ratings_allstates) if c not in ['state', 'PrimaryKey', 'mukey']]

for var in soil_variables:
    if var == 'BdSurf_DCD_SL':
        var_solus = 'dbovendry_0_cm_p'
        df_solus[var_solus] = df_solus[var_solus]/ 100.
    elif var == 'Dep2BedRS_WA':
        var_solus = 'anylithicdpt_cm_p'
    elif var == 'Dep2AnyRes_WA':
        var_solus = 'resdept_all_cm_p'
    else:
        continue

    if False:
        missing_primarykeys = df_ratings_allstates.loc[df_ratings_allstates[var].isna(), 'PrimaryKey']
        for i, primarykey in enumerate(missing_primarykeys):
            print(f'{var}: {i+1}/{len(missing_primarykeys)}')
            df_ratings_allstates.loc[df_ratings_allstates.PrimaryKey==primarykey, var] = (
                df_solus.loc[df_solus.PrimaryKey==primarykey, var_solus].values[0])
            missing_key_after_filling = len(df_ratings_allstates[var].isna())
            print(f'after filling wtih solus, number of missing points: {missing_key_after_filling}')


    if True:
        missing_mask = df_ratings_allstates[var].isna()
        missing_before = missing_mask.sum()
        print(f'{var}: {missing_before} missing values')        
        if missing_before > 0:
            solus_sub = df_solus[['PrimaryKey', var_solus]].dropna()
            temp_df = df_ratings_allstates[['PrimaryKey']].merge(
                solus_sub, how='left', on='PrimaryKey')
            df_ratings_allstates.loc[missing_mask, var] = temp_df.loc[missing_mask, var_solus]
            
            missing_after = df_ratings_allstates[var].isna().sum()
            print(f'After filling with solus: {missing_after} missing values remaining\n')

    df_ratings_allstates.to_csv(os.path.join(ROOT, 'output_bd_v2', 'all_states_bd_solus_filled.csv'), index=False)




df_ratings_allstates.to_excel(os.path.join(ROOT, 'output_bd_v2', 'all_states_bd_solus2.xlsx'))
