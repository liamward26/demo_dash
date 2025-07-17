import os
import base64
import json
from census import Census
import datetime
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe

encoded = os.getenv("GOOGLE_CREDS")
if not encoded:
    raise ValueError("GOOGLE_CREDS not set")

# Decode it into bytes
decoded = base64.b64decode(encoded)

# Option A: Load credentials directly from the JSON bytes
creds_info = json.loads(decoded)

API_KEY = os.getenv('CENSUS_KEY')
if not API_KEY:
    raise ValueError("Please set the CENSUS_API_KEY environment variable.")
c = Census(API_KEY)

def get_most_recent_acs_year(census_client, min_year=2010, max_year=None):
    if max_year is None:
        max_year = datetime.datetime.now().year
    for year in range(max_year, min_year - 1, -1):
        try:
            # Try a simple request for a known variable and geography
            test = census_client.acs5.us(("B01003_001E",), year=year)
            if test and isinstance(test, list):
                return year
        except Exception:
            continue
    raise ValueError("No ACS data found in the given year range.")

# Usage:
most_recent_year = get_most_recent_acs_year(c)
years = list(range(most_recent_year - 9, most_recent_year + 1))  # Last 10 years

# --- Combined and deduplicated variables ---
vars_named = {
    "population": "B01003_001E",
    "total_households": "B19001_001E",
    "total_families": "B17010_001E",
    "families_below_poverty": "B17010_002E",
    "income_10k_less": "B19001_002E",
    "income_10_15k": "B19001_003E",
    "income_15_20k": "B19001_004E",
    "income_20_25k": "B19001_005E",
    "income_25_30k": "B19001_006E",
    "income_30_35k": "B19001_007E",
    "income_35_40k": "B19001_008E",
    "income_40_45k": "B19001_009E",
    "income_45_50k": "B19001_010E",
    "income_50_60k": "B19001_011E",
    "income_60_75k": "B19001_012E",
    "income_75_100k": "B19001_013E",
    "income_100_125k": "B19001_014E",
    "income_125_150k": "B19001_015E",
    "income_150_200k": "B19001_016E",
    "income_200k_more": "B19001_017E",
    # Educational attainment (B15003, all 25 categories)
    "edu_total_25plus": "B15003_001E",
    "edu_none": "B15003_002E",
    "edu_nursery": "B15003_003E",
    "edu_kindergarten": "B15003_004E",
    "edu_1st_grade": "B15003_005E",
    "edu_2nd_grade": "B15003_006E",
    "edu_3rd_grade": "B15003_007E",
    "edu_4th_grade": "B15003_008E",
    "edu_5th_grade": "B15003_009E",
    "edu_6th_grade": "B15003_010E",
    "edu_7th_grade": "B15003_011E",
    "edu_8th_grade": "B15003_012E",
    "edu_9th_grade": "B15003_013E",
    "edu_10th_grade": "B15003_014E",
    "edu_11th_grade": "B15003_015E",
    "edu_12th_no_diploma": "B15003_016E",
    "edu_hs_grad": "B15003_017E",
    "edu_ged": "B15003_018E",
    "edu_some_college_less_1yr": "B15003_019E",
    "edu_some_college_more_1yr": "B15003_020E",
    "edu_assoc": "B15003_021E",
    "edu_bachelor": "B15003_022E",
    "edu_master": "B15003_023E",
    "edu_professional": "B15003_024E",
    "edu_doctorate": "B15003_025E",
    # Housing units & tenure
    "renter_units": "B25003_003E",
    "owner_units": "B25003_002E",
    "renter_pop": "B25008_003E",
    "owner_pop": "B25008_002E",
    # Vacancy
    "vacant_sum": "B25004_001E",
    "vacant_for_rent": "B25004_002E",
    "vacant_rented_not_occupied": "B25004_003E",
    "vacant_for_sale": "B25004_004E",
    "vacant_sold_not_occupied": "B25004_005E",
    "vacant_for_seasonal": "B25004_006E",
    "vacant_for_migrant": "B25004_007E",
    "vacant_other": "B25004_008E",
    # Structure type (owned & rented)
    "structures_own_1_detached_unit": "B25032_003E",
    "structures_own_1_attached_unit": "B25032_004E",
    "structures_own_2_unit": "B25032_005E",
    "structures_own_3_to_4_units": "B25032_006E",
    "structures_own_5_to_9_units": "B25032_007E",
    "structures_own_10_to_19_units": "B25032_008E",
    "structures_own_20_to_49_units": "B25032_009E",
    "structures_own_50_or_more_units": "B25032_010E",
    "structures_own_mobile_homes": "B25032_011E",
    "structures_own_other": "B25032_012E",
    "structures_rent_1_detached_unit": "B25032_014E",
    "structures_rent_1_attached_unit": "B25032_015E",
    "structures_rent_2_unit": "B25032_016E",
    "structures_rent_3_to_4_units": "B25032_017E",
    "structures_rent_5_to_9_units": "B25032_018E",
    "structures_rent_10_to_19_units": "B25032_019E",
    "structures_rent_20_to_49_units": "B25032_020E",
    "structures_rent_50_or_more_units": "B25032_021E",
    "structures_rent_mobile_homes": "B25032_022E",
    "structures_rent_other": "B25032_023E",
    # Race of householder
    "race_white_households": "B25006_002E",
    "race_black_households": "B25006_003E",
    "race_AI_AN_households": "B25006_004E",
    "race_asian_households": "B25006_005E",
    "race_NHPI_households": "B25006_006E",
    "race_other_households": "B25006_007E",
    "race_two_or_more_households": "B25006_008E",
    # Occupancy
    "occupied_units": "B25002_002E",
    "vacant_units": "B25002_003E",
    # Median values
    "median_rent": "B25064_001E",
    "median_house_value": "B25077_001E",
    "med_income": "B19013_001E",
    "median_income": "B19013_001E"
}
"""
    APPEND THIS SECTION FOR MOST RECENT YEAR
    # Year built
    "2020s_units": "B25034_002E",
    "2010s_units": "B25034_003E",
    "2000s_units": "B25034_004E",
    "1990s_units": "B25034_005E",
    "1980s_units": "B25034_006E",
    "1970s_units": "B25034_007E",
    "1960s_units": "B25034_008E",
    "1950s_units": "B25034_009E",
    "1940s_units": "B25034_010E",
    "1939_or_earlier_units": "B25034_011E",
    # Gross rent brackets
    "gross_rent_under_100": "B25063_003E",
    "gross_rent_100_150": "B25063_004E",
    "gross_rent_150_200": "B25063_005E",
    "gross_rent_200_250": "B25063_006E",
    "gross_rent_250_300": "B25063_007E",
    "gross_rent_300_350": "B25063_008E",
    "gross_rent_350_400": "B25063_009E",
    "gross_rent_400_450": "B25063_010E",
    "gross_rent_450_500": "B25063_011E",
    "gross_rent_500_550": "B25063_012E",
    "gross_rent_550_600": "B25063_013E",
    "gross_rent_600_650": "B25063_014E",
    "gross_rent_650_700": "B25063_015E",
    "gross_rent_700_750": "B25063_016E",
    "gross_rent_750_800": "B25063_017E",
    "gross_rent_800_850": "B25063_018E",
    "gross_rent_850_900": "B25063_019E",
    "gross_rent_900_1000": "B25063_020E",
    "gross_rent_1000_1250": "B25063_021E",
    "gross_rent_1250_1500": "B25063_022E",
    "gross_rent_1500_2000": "B25063_023E",
    "gross_rent_2000_2500": "B25063_024E",
    "gross_rent_2500_3000": "B25063_025E",
    "gross_rent_3000_3500": "B25063_026E",
    "gross_rent_3500_plus": "B25063_027E",
    # Rent burden (B25074)
    "burden_rent_under_10k_sum": "B25074_002E",
    "burden_rent_under_10k_30_35p": "B25074_006E",
    "burden_rent_under_10k_35_40p": "B25074_007E",
    "burden_rent_under_10k_40_50p": "B25074_008E",
    "burden_rent_under_10k_50_plusp": "B25074_009E",
    "burden_rent_10k_to_20k_sum": "B25074_011E",
    "burden_rent_10k_to_20k_30_35p": "B25074_015E",
    "burden_rent_10k_to_20k_35_40p": "B25074_016E",
    "burden_rent_10k_to_20k_40_50p": "B25074_017E",
    "burden_rent_10k_to_20k_50_plusp": "B25074_018E",
    "burden_rent_20k_to_35k_sum": "B25074_020E",
    "burden_rent_20k_to_35k_30_35p": "B25074_024E",
    "burden_rent_20k_to_35k_35_40p": "B25074_025E",
    "burden_rent_20k_to_35k_40_50p": "B25074_026E",
    "burden_rent_20k_to_35k_50_plusp": "B25074_027E",
    "burden_rent_35k_to_50k_sum": "B25074_029E",
    "burden_rent_35k_to_50k_30_35p": "B25074_033E",
    "burden_rent_35k_to_50k_35_40p": "B25074_034E",
    "burden_rent_35k_to_50k_40_50p": "B25074_035E",
    "burden_rent_35k_to_50k_50_plusp": "B25074_036E",
    "burden_rent_50k_to_75k_sum": "B25074_038E",
    "burden_rent_50k_to_75k_30_35p": "B25074_042E",
    "burden_rent_50k_to_75k_35_40p": "B25074_043E",
    "burden_rent_50k_to_75k_40_50p": "B25074_044E",
    "burden_rent_50k_to_75k_50_plusp": "B25074_045E",
    "burden_rent_75k_to_100k_sum": "B25074_047E",
    "burden_rent_75k_to_100k_30_35p": "B25074_051E",
    "burden_rent_75k_to_100k_35_40p": "B25074_052E",
    "burden_rent_75k_to_100k_40_50p": "B25074_053E",
    "burden_rent_75k_to_100k_50_plusp": "B25074_054E",
    "burden_rent_over_100k_sum": "B25074_056E",
    "burden_rent_over_100k_30_35p": "B25074_060E",
    "burden_rent_over_100k_35_40p": "B25074_061E",
    "burden_rent_over_100k_40_50p": "B25074_062E",
    "burden_rent_over_100k_50_plusp": "B25074_063E",
"""

def rename_vars(df, vars_dict):
    return df.rename(columns={v: k for k, v in vars_dict.items()})

target_places = [
    'Chesapeake city', 'Hampton city', 'Newport News city', 'Norfolk city',
    'Portsmouth city', 'Suffolk city', 'Virginia Beach city',
    'James City County', 'York County'
]
target_names = [f"{city}, Virginia" for city in target_places]
target_names.append('Virginia Beach-Chesapeake-Norfolk, VA-NC Metro Area')
target_names += ["Virginia", "United States"]

all_results = []

for year in years:
    # VA counties
    va_rows = c.acs5.get(
        tuple(["NAME"] + list(vars_named.values())),
        {'for': 'county:*', 'in': 'state:51'},
        year=year
    )
    for row in va_rows:
        if row['NAME'] in target_names:
            row['year'] = year
            row['state'] = 'Virginia'
            all_results.append(row)
    # State of VA
    va_state = c.acs5.state(tuple(vars_named.values()), '51', year=year)
    va_state_dict = va_state[0]
    va_state_dict['NAME'] = "Virginia"
    va_state_dict['year'] = year
    va_state_dict['state'] = "Virginia"
    all_results.append(va_state_dict)
    # US
    us = c.acs5.us(tuple(vars_named.values()), year=year)
    us_dict = us[0]
    us_dict['NAME'] = "United States"
    us_dict['year'] = year
    us_dict['state'] = "United States"
    all_results.append(us_dict)

# Build single DataFrame
df_all_years = pd.DataFrame(all_results)
df_all_years = rename_vars(df_all_years, vars_named)
df_all_years = df_all_years[df_all_years['NAME'].isin(target_names)]
df_all_years['year'] = pd.to_datetime(df_all_years['year'], format="%Y")

# Example: Write to Google Sheets (first worksheet)
credentials = json.loads(base64.b64decode(os.getenv('GOOGLE_CREDS')))
gc = gspread.service_account_from_dict(credentials)
sh = gc.open('dash_demo')
worksheet = sh.get_worksheet(0)
set_with_dataframe(worksheet, df_all_years)
