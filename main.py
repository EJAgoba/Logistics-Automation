import pandas as pd
from location_codes_finder import Location_Codes_Finder
from matrix_mapping import MatrixMapper
from file_reader import read_any_file


accrual_sheet = read_any_file(r"C:\Users\c1354623\OneDrive - Cintas Corporation\Documents\Miscellaneous_Excel_Analysis\New Accrual Logic III\SAP_Weekly_Audit_Detail_01162026.txt")     # or .csv or .txt


# References
cintas_master_data = pd.read_excel(r"C:\Users\c1354623\OneDrive - Cintas Corporation\Documents\Miscellaneous_Excel_Analysis\New Accrual Logic III\MY LOCATION TABLE (4).xlsx")
cintas_master_data_2 = pd.read_excel(r"C:\Users\c1354623\OneDrive - Cintas Corporation\Documents\Miscellaneous_Excel_Analysis\New Accrual Logic III\Master Location Table.xlsx")
cintas_location_codes = pd.read_excel(r"C:\Users\c1354623\OneDrive - Cintas Corporation\Documents\Miscellaneous_Excel_Analysis\New Accrual Logic III\all_location_codes.xlsx")
cintas_coding_matrix = ""

location_finder = Location_Codes_Finder(master_table=cintas_master_data, codes_list=cintas_location_codes["Codes"])

### we are going to combine addresses first before getting any location code
master_combined_set = set(cintas_master_data.apply(lambda r: location_finder.combine_addr(r["Loc_Address"], r["Loc_City"], r["Loc_ST"]), axis=1).dropna())
accrual_sheet["Consignor_Combined_Address"] = accrual_sheet.apply(lambda r: location_finder.combine_addr(r["Origin Addresss"], r["Origin City"], r["Origin State Code"]), axis=1)
accrual_sheet["Consignee_Combined_Address"] = accrual_sheet.apply(lambda r: location_finder.combine_addr(r["Dest Address1"], r["Dest City"], r["Dest State Code"]), axis=1)

# Location_Code_Extractor()
"""The function of the Location Code Extractor is to extract any location codes that it finds in Consignor and Consignee Columns"""
### this returns a location code
accrual_sheet['Extracted Consignor Code'] = accrual_sheet["Consignor"].apply(location_finder.extract_from_text)
accrual_sheet['Extracted Consignee Code'] = accrual_sheet["Consignee"].apply(location_finder.extract_from_text)

# Address_Checker()
"""The function of the Address Checker is ONLY to check if an address in the Accrual or Weekly Audit Sheet exists in the Cintas Master Data. It should be a YES/NO, or EXISTS/DOESN'T EXIST
So this address checker will apply only to rows where Re-Coded Consignor and Re-Coded Consignee is NOT BLANK
"""
### this returns a yes or no and is used under Location_Code_Extractor
consignor_has_code = (
   accrual_sheet["Extracted Consignor Code"].notna() &
   (accrual_sheet["Extracted Consignor Code"].astype(str).str.strip() != "")
)
consignee_has_code = (
   accrual_sheet["Extracted Consignee Code"].notna() &
   (accrual_sheet["Extracted Consignee Code"].astype(str).str.strip() != "")
)
# 2) Build masks: name does NOT mention CINTAS or MAT
consignor_no_cintas_or_mat = ~accrual_sheet["Consignor"].fillna("").str.upper().str.contains(r"\b(CINTAS|MAT)\b", regex=True)
consignee_no_cintas_or_mat = ~accrual_sheet["Consignee"].fillna("").str.upper().str.contains(r"\b(CINTAS|MAT)\b", regex=True)
# 3) Build masks: combined address exists in master
consignor_addr_ok = accrual_sheet["Consignor_Combined_Address"].fillna("").isin(master_combined_set)
consignee_addr_ok = accrual_sheet["Consignee_Combined_Address"].fillna("").isin(master_combined_set)
# 4) Wipe codes ONLY when:
#    code exists + not cintas/mat + address NOT in master
wipe_consignor = consignor_has_code & consignor_no_cintas_or_mat & ~consignor_addr_ok
wipe_consignee = consignee_has_code & consignee_no_cintas_or_mat & ~consignee_addr_ok
accrual_sheet.loc[wipe_consignor, "Extracted Consignor Code"] = ""
accrual_sheet.loc[wipe_consignee, "Extracted Consignee Code"] = ""


# Location_Code_Based_On_OrgType/DestType_Finder()
# """The function of the Location_Code_Based_On_OrgType/DestType_Finder() is to find a location code based on the Org Type Code and Dest Type Code Colunms"""
accrual_sheet[["Org Type Consignor Code", "Dest Type Consignee Code"]] = accrual_sheet.apply(lambda r: pd.Series(location_finder.extract_from_org_dest_type(r["Org Type Code"], r["Dest Type Code"])), axis=1)


# Location_Code_Based_On_Address_Finder()
"""The function of the Location_Code_Based_On_Address_Finder is to find a location code based on the address in the accrual or weekly audit sheet that it finds in the cintas master data"""
### this returns a location code. Thus, this and location code extractor should probably be under one class called "Location_Code_Finder"
accrual_sheet["Addr_Lookup_Consignor_Code"] = accrual_sheet["Consignor_Combined_Address"].apply(
   location_finder.extract_from_address
)

accrual_sheet["Addr_Lookup_Consignee_Code"] = accrual_sheet["Consignee_Combined_Address"].apply(
   location_finder.extract_from_address
)

# Final Location Code
"""
This compares the extraction location codes with the address-based location code. 
So there'll be a new column called Final Location Code, where if extracted location code is filled, it uses that, 
if it's not and address-based is filled, then use address, based, if none of them is filled, say 'Non-Cintas'
"""
import numpy as np
def clean_blank(s):
   return s.replace(r"^\s*$", pd.NA, regex=True)
accrual_sheet["Final Consignor Code"] = (
   clean_blank(accrual_sheet["Extracted Consignor Code"])
   .fillna(clean_blank(accrual_sheet["Org Type Consignor Code"]))
   .fillna(clean_blank(accrual_sheet["Addr_Lookup_Consignor_Code"]))
   .fillna("NON-CINTAS")
)
accrual_sheet["Final Consignee Code"] = (
   clean_blank(accrual_sheet["Extracted Consignee Code"])
   .fillna(clean_blank(accrual_sheet["Dest Type Consignee Code"]))
   .fillna(clean_blank(accrual_sheet["Addr_Lookup_Consignee_Code"]))
   .fillna("NON-CINTAS")
)

# Type_Code_Finder()
"""The function of the Type_Code_Finder is ONLY to find the type code of each value in the Re-coded Consignor and Consignee Code Columns"""
### this returns a type code
master_code_to_type = dict(
   zip(
       cintas_master_data_2["Loc Code"].astype(str).str.upper(),
       cintas_master_data_2["Type Code"]
           .astype(str)
           .str.upper()
           .replace("NAN", pd.NA)   # fixes string "nan" cases
   )
)

accrual_sheet["Final Consignor Type"] = accrual_sheet["Final Consignor Code"].map(master_code_to_type)
accrual_sheet["Final Consignee Type"] = accrual_sheet["Final Consignee Code"].map(master_code_to_type)
accrual_sheet["Final Consignor Type"] = accrual_sheet["Final Consignor Type"].fillna("NON-CINTAS")
accrual_sheet["Final Consignee Type"] = accrual_sheet["Final Consignee Type"].fillna("NON-CINTAS")

# Responsible_Party_Decider()
"""The function of the Responsible Party Decider is to find the responsible party of each re-coded Consignor and Consignee pair based on the type codes and the coding matrix"""
### this returns a location code, but that location code is a responsible party, so should probably be under its own class
mapper = MatrixMapper()
accrual_sheet["Responsible Party"] = accrual_sheet.apply(
   mapper.determine_profit_center,
   axis=1
)

# Profit_Center_Populator()
"""The function of the Profit_Center_Populator is ONLY to populate the responsible party in the Profit_Center_Populator column"""

"""
The function of the Profit_Center_Populator is ONLY to populate 
the responsible party in the Profit_Center_Populator column
"""
# Merge profit and cost center data (force suffixes)
accrual_sheet = accrual_sheet.merge(
   cintas_master_data_2[["Loc Code", "ProfitCtr", "Cost Center"]],
   left_on="Responsible Party",
   right_on="Loc Code",
   how="left",
   suffixes=("", "_master")
)
# Rename ONLY the master fields
accrual_sheet.rename(
   columns={
       "ProfitCtr": "Profit Center EJ",
       "Cost Center_master": "Cost Center EJ"
   },
   inplace=True
)
# Convert to string safely (keep blanks blank)
accrual_sheet["Profit Center EJ"] = accrual_sheet["Profit Center EJ"].fillna("").astype(str)
accrual_sheet["Cost Center EJ"] = accrual_sheet["Cost Center EJ"].fillna("").astype(str)

# GL_Code_Populator()
"""The function of the GL_Code_Populator is ONLY to populate the responsible party in the GL_Code_Populator column"""
accrual_sheet["Account # EJ"] = accrual_sheet.apply(
   lambda row: 621000
   if "G59" in str(row.get("Profit Center EJ", ""))
   else (
         621000
         if row.get("Final Consignee Code") == row.get("Responsible Party")
         else 621020
   ),
   axis=1,
)

# Automation Accuracy
if {"Profit Center", "Profit Center EJ"}.issubset(accrual_sheet.columns):
   match = (
         (accrual_sheet["Profit Center"] == accrual_sheet["Profit Center EJ"])
& accrual_sheet["Profit Center"].notna()
& accrual_sheet["Profit Center EJ"].notna()
   )
   accrual_sheet["Automation Accuracy"] = match.astype(int)
else:
   accrual_sheet["Automation Accuracy"] = 0


# Column Reordering
first_cols = [
   "Profit Center",
   "Cost Center",
   "Account #",
   "Automation Accuracy",
   "Profit Center EJ",
   "Cost Center EJ",
   "Account # EJ"
]
# keep only the ones that actually exist (prevents errors)
first_cols = [c for c in first_cols if c in accrual_sheet.columns]
# everything else after
other_cols = [c for c in accrual_sheet.columns if c not in first_cols]
# reorder
accrual_sheet = accrual_sheet[first_cols + other_cols]


# Output
output_file = "Weekly Batch 463.xlsx"
accrual_sheet.to_excel(output_file, index=False)


