from pathlib import Path

''' Use this script to check if all the states are in the database
you only need 46'''

folder = Path(r"D:\work\data\ssurgo_download\DATABSE20251213")

ALL_50_STATES = {
    "Alabama","Alaska","Arizona","Arkansas","California","Colorado","Connecticut","Delaware",
    "Florida","Georgia","Hawaii","Idaho","Illinois","Indiana","Iowa","Kansas","Kentucky","Louisiana",
    "Maine","Maryland","Massachusetts","Michigan","Minnesota","Mississippi","Missouri","Montana",
    "Nebraska","Nevada","New Hampshire","New Jersey","New Mexico","New York","North Carolina",
    "North Dakota","Ohio","Oklahoma","Oregon","Pennsylvania","Rhode Island","South Carolina",
    "South Dakota","Tennessee","Texas","Utah","Vermont","Virginia","Washington","West Virginia",
    "Wisconsin","Wyoming" }

print(f"Checking {len(ALL_50_STATES)} states")
found = set()

for p in folder.iterdir():
    if p.is_dir():
        name = p.name
        if name.lower().endswith("_gpkg"):
            name = name[:-5]  # drop "_gpkg"
        state = name.replace("_", " ").strip()
        # match against canonical names (case-insensitive)
        for s in ALL_50_STATES:
            if state.lower() == s.lower():
                found.add(s)
                break

missing = sorted(ALL_50_STATES - found)

print(f"Found: {len(found)} states")
print(f"Missing: {len(missing)} states")
print("Missing list:", missing)
