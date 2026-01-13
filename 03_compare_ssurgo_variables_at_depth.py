import pandas as pd
import sys
import os


''' this is to compare the soil variable values at different depths
and to decide: what depth to use and which variable to use (Health or Physcial for example)'''


root = r'D:\work\data\ssurgo_download\DATABSE20251213_excel'
output_dir = os.path.join(root, 'output')
input_dir = os.path.join(root, 'input')

def compare_sheets(excel_file):
    pairs = [
        ('omr_WA_0_4_cm', 'OrgMatter_WA_0_4_cm',
         'omr_WA_0_4_cm', 'OrgMatter_WA_0_4_cm',
         'pctMU_omr_WA_0_4_cm', 'pctMU_OrgMatter_WA_0_4_cm'),
        
        ('omr_WA_0_10_cm', 'OrgMatter_WA_0_10_cm',
         'omr_WA_0_10_cm', 'OrgMatter_WA_0_10_cm',
         'pctMU_omr_WA_0_10_cm', 'pctMU_OrgMatter_WA_0_10_cm'),
        
        ('omr_WA_SL', 'OrgMatter_WA_SL',
         'omr_WA_SL', 'OrgMatter_WA_SL',
         'pctMU_omr_WA_SL', 'pctMU_OrgMatter_WA_SL'),
    ]
    
    all_sheets = pd.read_excel(excel_file, sheet_name=None)
    results = []
    
    # compare two sets of organic matter
    if True:
        for sheet1, sheet2, val1, val2, pct1, pct2 in pairs:
            print(f"\n{'='*60}")
            print(f"Comparing: {sheet1} vs {sheet2}")
            print('='*60)
            
            df1 = all_sheets[sheet1]
            df2 = all_sheets[sheet2]
            print(f"Number of NaN in {sheet1}: {df1.isna().sum().sum()}")
            print(f"Number of NaN in {sheet2}: {df2.isna().sum().sum()}")
            
            var1 = sheet1
            var2 = sheet2

            df_merged = pd.merge(df1, df2, on='mukey')
            df_merged = df_merged[['mukey', var1, var2, 'pctMU_'+var1, 'pctMU_'+var2]]
            df_merged['diff'] = df_merged[var1] - df_merged[var2]
            df_merged['diff_pctMU'] = df_merged['pctMU_'+var1] - df_merged['pctMU_'+var2]
            df_merged.to_csv(os.path.join(output_dir, f'compare_{sheet1}.csv'))

    if True:  # compare 4cm and 10cm and SL

        df1 = all_sheets['omr_WA_0_4_cm'][['mukey', 'omr_WA_0_4_cm']]
        df2 = all_sheets['omr_WA_0_10_cm'][['mukey', 'omr_WA_0_10_cm']]
        df3 = all_sheets['omr_WA_0_20_cm'][['mukey', 'omr_WA_0_20_cm']]
        df4 = all_sheets['omr_WA_SL'][['mukey', 'omr_WA_SL']]
        df_merged = pd.merge(df1, df2, on='mukey')
        df_merged = pd.merge(df_merged, df3, on='mukey')
        df_merged = pd.merge(df_merged, df4, on='mukey')
        df_merged.to_excel(os.path.join(output_dir, f'compare_depth.xlsx'))

if __name__ == '__main__':
    file_path = os.path.join(input_dir, 'nb.xlsx')

    compare_sheets(file_path)