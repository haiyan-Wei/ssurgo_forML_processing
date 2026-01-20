"""
Exploratory Data Analysis Script
Analyzes distributions, missing data, and outliers for soil/geological data
"""

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

excel_file = r"B:\work_subset\projects\src\ssurgo\outputs\v2\05_combine_ssurgo_solus\ssurgo_solus_combined.xlsx"

output_dir = r'B:\work_subset\projects\src\ssurgo\outputs\v2\05_combine_ssurgo_solus'
df = pd.read_excel(excel_file, engine='openpyxl')

def plot_soil_distributions(df, output_dir):
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
    


print_summary_stats(df)
plot_soil_distributions(df, output_dir)

