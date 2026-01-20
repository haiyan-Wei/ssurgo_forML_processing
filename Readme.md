# SSURGO Processing Pipeline

## Overview
Processes bulk SSURGO data into state-level geopackages, extracts soil variables, and combines with SOLUS.

## Steps
1. download bulk raw data in arcpro with tbx tool
2. create gpkg for each state
    - use `01_check_missing_state_pkgp.py` to check if missed any state
4. extract soil variables and store them in gpkg for each state
    - use `02_get_rating_tables_summary.py` to get a summary table see what's missing for each state
    - run `03_compare_ssurgo_variables_at_depth.py` to decide what depth to use
5. use `04_extract_rating_tables_from_a_variable_list.py`
6. run `05_combine_ssurgo_with_solus.py`


## Important
- SSURGO gpkg (raw data from step 1/2) doesn't follow exactly state boarders. so do not use state boundaries
- use points `nri66kpoints_prj_ssurgo` for projection consistency
