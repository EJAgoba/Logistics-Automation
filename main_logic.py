# main_logic.py
from __future__ import annotations

import re
from io import BytesIO, StringIO
from typing import Optional

import pandas as pd

from location_codes_finder import Location_Codes_Finder
from matrix_mapping import MatrixMapper


# =========================
# File Reader (upload-safe)
# =========================
def read_uploaded_to_df(uploaded_file) -> pd.DataFrame:
    name = (getattr(uploaded_file, "name", "") or "").lower()
    raw_bytes = uploaded_file.getvalue()

    # Excel
    if name.endswith((".xlsx", ".xls", ".xlsm")):
        return pd.read_excel(BytesIO(raw_bytes))

    # CSV
    if name.endswith(".csv"):
        try:
            return pd.read_csv(BytesIO(raw_bytes), encoding="utf-8")
        except Exception:
            return pd.read_csv(BytesIO(raw_bytes), encoding="latin1")

    # TXT
    if name.endswith(".txt"):
        if b"\x00" in raw_bytes:
            try:
                text = raw_bytes.decode("utf-16")
            except Exception:
                cleaned = raw_bytes.replace(b"\x00", b"")
                try:
                    text = cleaned.decode("utf-8")
                except Exception:
                    text = cleaned.decode("latin1")
        else:
            try:
                text = raw_bytes.decode("utf-8")
            except Exception:
                text = raw_bytes.decode("latin1")

        sio = StringIO(text)

        # Try common delimiters first
        for sep in ["\t", "|", ",", ";"]:
            sio.seek(0)
            try:
                df = pd.read_csv(
                    sio,
                    sep=sep,
                    engine="python",
                    on_bad_lines="skip",
                    quoting=3,
                )
                if df.shape[1] > 1:
                    return df
            except Exception:
                continue

        # Fixed-width fallback
        sio.seek(0)
        try:
            return pd.read_fwf(sio)
        except Exception:
            pass

        # Final fallback
        sio.seek(0)
        return pd.read_csv(sio, sep=None, engine="python", on_bad_lines="skip")

    raise ValueError("Unsupported file type. Upload .xlsx, .csv, or .txt")


# =========================
# Column Standardization
# =========================
def _norm_col(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(s).lower())


def _pick_col(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    norm_map = {_norm_col(c): c for c in df.columns}
    for cand in candidates:
        key = _norm_col(cand)
        if key in norm_map:
            return norm_map[key]
    return None


def standardize_input_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates canonical columns used by the pipeline regardless of input naming.
    Canonical fields created:
      - Consignor, Consignee
      - Origin Address, Origin City, Origin State
      - Destination Address, Destination City, Destination State
    """
    df = df.copy()

    FIELD_ALIASES = {
        "Consignor": [
            "Consignor", "Origin Facility", "Shipper", "Ship From Name", "Origin Name", "Org Name",
            "Origin Company", "Org Company", "ShipFromName", "OrgName",
        ],
        "Consignee": [
            "Consignee", "Destination Facility", "Receiver", "Ship To Name", "Destination Name", "Dest Name",
            "Dest Company", "Destination Company", "ShipToName", "DestName",
        ],
        "Origin Address": [
            "Origin Address", "Origin Address1", "Origin Addresss", "Org Address", "Org Address1",
            "Ship From Address", "Ship From Address1", "Shipper Address", "Shipper Address1",
        ],
        "Origin City": ["Origin City", "Org City", "Ship From City", "Shipper City"],
        "Origin State": [
            "Origin State", "Origin State Code", "Org State", "Org State Code",
            "Ship From State", "Ship From State Code",
        ],
        "Destination Address": [
            "Destination Address", "Destination Address1", "Dest Address", "Dest Address1",
            "Ship To Address", "Ship To Address1", "Receiver Address", "Receiver Address1",
        ],
        "Destination City": ["Destination City", "Dest City", "Ship To City", "Receiver City"],
        "Destination State": [
            "Destination State", "Destination State Code", "Dest State", "Dest State Code",
            "Ship To State", "Ship To State Code",
        ],
    }

    for canonical, aliases in FIELD_ALIASES.items():
        found = _pick_col(df, aliases)
        df[canonical] = df[found] if found else ""

    for c in [
        "Consignor", "Consignee",
        "Origin Address", "Origin City", "Origin State",
        "Destination Address", "Destination City", "Destination State",
    ]:
        df[c] = df[c].fillna("").astype(str)

    return df


# =========================
# Utilities
# =========================
def clean_blank(s: pd.Series) -> pd.Series:
    return s.replace(r"^\s*$", pd.NA, regex=True)


def _normalize_loc_code(v) -> str:
    """
    Normalizes location codes so purely numeric values become 4 chars.
    Examples:
      95 -> 0095
      972 -> 0972
      0095 -> 0095
      011K -> 011K
    """
    if v is None:
        return ""
    s = str(v).strip().upper()
    if not s:
        return ""
    if re.fullmatch(r"\d+", s):
        return s.zfill(4)
    return s


def _normalize_loc_code_series(s: pd.Series) -> pd.Series:
    return s.fillna("").astype(str).map(_normalize_loc_code)


def _as_text_keep_zeros(series: pd.Series, decimals: int = 5) -> pd.Series:
    """
    Convert numeric-ish values into text while preserving trailing zeros.
    Example: 312.1 -> 312.10000
    """
    s = series.fillna("").astype(str).str.strip()

    def fmt(v: str) -> str:
        if not v:
            return ""
        if not re.fullmatch(r"-?\d+(\.\d+)?", v):
            return v
        try:
            num = float(v)
        except Exception:
            return v
        return f"{num:.{decimals}f}"

    return s.map(fmt)


# =========================
# Main Pipeline
# =========================
def run_pipeline(
    accrual_sheet: pd.DataFrame,
    cintas_master_data: pd.DataFrame,       # MY LOCATION TABLE (4)
    cintas_master_data_2: pd.DataFrame,     # Master Location Table
    cintas_location_codes: pd.DataFrame,    # all_location_codes
) -> pd.DataFrame:
    accrual_sheet = accrual_sheet.copy()
    accrual_sheet = standardize_input_columns(accrual_sheet)

    # Clean stray quotes from original imported text
    for col in accrual_sheet.columns:
        if accrual_sheet[col].dtype == "object":
            accrual_sheet[col] = (
                accrual_sheet[col]
                .fillna("")
                .astype(str)
                .str.strip()
                .str.replace('"', '', regex=False)
            )

    # =========================
    # Audit / Non-Audit Split
    # =========================
    shipment_col = _pick_col(accrual_sheet, ["Shipment #", "Shipment Number", "Shipment No", "Shipment"])
    mode_col = _pick_col(accrual_sheet, ["Mode", "mode"])

    audit_modes = {"FA", "LT", "M", "O", "U", "LTL", "FTL", "TL"}
    audit_shipments = {"00950369549", "00004FY646", "000018WA68", "0000302AR0", "00004FY590", "000089V181", "0000R420V3", 
                       "0000R4864V", "0000RV2559", "0000V10585", "0000W0A387", "0000WA3182", "244978215", "452310449", 
                       "454238800", "455119006", "455344042"}

    if mode_col:
        mode_mask = (
            accrual_sheet[mode_col]
            .fillna("")
            .astype(str)
            .str.upper()
            .str.strip()
            .isin(audit_modes)
        )
    else:
        mode_mask = pd.Series(False, index=accrual_sheet.index)

    if shipment_col:
        shipment_mask = (
            accrual_sheet[shipment_col]
            .fillna("")
            .astype(str)
            .str.strip()
            .isin(audit_shipments)
        )
    else:
        shipment_mask = pd.Series(False, index=accrual_sheet.index)

    audit_mask = mode_mask | shipment_mask

    audit_df = accrual_sheet[audit_mask].copy()
    non_audit_df = accrual_sheet[~audit_mask].copy()

    audit_df["Category"] = "Audit"
    non_audit_df["Category"] = "Non Audit"

    # If nothing qualifies, return everything as Non Audit
    if audit_df.empty:
        return non_audit_df

    # --- Defensive normalization of reference tables
    my_loc = cintas_master_data.copy()
    master_loc = cintas_master_data_2.copy()
    all_codes = cintas_location_codes.copy()

    if "Loc Code" in my_loc.columns:
        my_loc["Loc Code"] = _normalize_loc_code_series(my_loc["Loc Code"])
    if "Loc Code" in master_loc.columns:
        master_loc["Loc Code"] = _normalize_loc_code_series(master_loc["Loc Code"])

    codes_col = "Codes" if "Codes" in all_codes.columns else all_codes.columns[0]
    all_codes[codes_col] = all_codes[codes_col].fillna("").astype(str).str.strip().str.upper()

    # --- Init services
    location_finder = Location_Codes_Finder(
        master_table=my_loc,
        codes_list=all_codes[codes_col],
    )

    # --- Combined address setup (master set)
    master_combined_set = set(
        my_loc.apply(
            lambda r: location_finder.combine_addr(
                r.get("Loc_Address", ""),
                r.get("Loc_City", ""),
                r.get("Loc_ST", "")
            ),
            axis=1,
        ).dropna()
    )

    # --- Audit combined addresses
    audit_df["Consignor_Combined_Address"] = audit_df.apply(
        lambda r: location_finder.combine_addr(r["Origin Address"], r["Origin City"], r["Origin State"]),
        axis=1,
    )
    audit_df["Consignee_Combined_Address"] = audit_df.apply(
        lambda r: location_finder.combine_addr(r["Destination Address"], r["Destination City"], r["Destination State"]),
        axis=1,
    )

    # --- Extract codes from text
    audit_df["Extracted Consignor Code"] = _normalize_loc_code_series(
        audit_df["Consignor"].apply(location_finder.extract_from_text)
    )
    audit_df["Extracted Consignee Code"] = _normalize_loc_code_series(
        audit_df["Consignee"].apply(location_finder.extract_from_text)
    )

    # --- Address checker rule
    consignor_has_code = audit_df["Extracted Consignor Code"].notna() & (
        audit_df["Extracted Consignor Code"].astype(str).str.strip() != ""
    )
    consignee_has_code = audit_df["Extracted Consignee Code"].notna() & (
        audit_df["Extracted Consignee Code"].astype(str).str.strip() != ""
    )

    consignor_no_cintas_or_mat = ~audit_df["Consignor"].fillna("").str.upper().str.contains(
        r"\b(CINTAS|MAT)\b", regex=True
    )
    consignee_no_cintas_or_mat = ~audit_df["Consignee"].fillna("").str.upper().str.contains(
        r"\b(CINTAS|MAT)\b", regex=True
    )

    consignor_addr_ok = audit_df["Consignor_Combined_Address"].fillna("").isin(master_combined_set)
    consignee_addr_ok = audit_df["Consignee_Combined_Address"].fillna("").isin(master_combined_set)

    wipe_consignor = consignor_has_code & consignor_no_cintas_or_mat & ~consignor_addr_ok
    wipe_consignee = consignee_has_code & consignee_no_cintas_or_mat & ~consignee_addr_ok

    audit_df.loc[wipe_consignor, "Extracted Consignor Code"] = ""
    audit_df.loc[wipe_consignee, "Extracted Consignee Code"] = ""

    # --- Extract from Org/Dest Type columns
    org_type_col = _pick_col(audit_df, ["Org Type Code", "Org Loc Code", "Origin Type Code", "OrgTypeCode", "Origin Location Code"])
    dest_type_col = _pick_col(audit_df, ["Dest Type Code", "Dest Loc Code", "Destination Type Code", "DestTypeCode", "Destination Location Code"])

    if org_type_col and dest_type_col:
        tmp = audit_df.apply(
            lambda r: pd.Series(
                location_finder.extract_from_org_dest_type(
                    r.get(org_type_col, ""),
                    r.get(dest_type_col, ""),
                )
            ),
            axis=1,
        )
        tmp.columns = ["Org Type Consignor Code", "Dest Type Consignee Code"]
        audit_df[["Org Type Consignor Code", "Dest Type Consignee Code"]] = tmp
    else:
        audit_df["Org Type Consignor Code"] = ""
        audit_df["Dest Type Consignee Code"] = ""

    audit_df["Org Type Consignor Code"] = _normalize_loc_code_series(audit_df["Org Type Consignor Code"])
    audit_df["Dest Type Consignee Code"] = _normalize_loc_code_series(audit_df["Dest Type Consignee Code"])

    # --- Address looked-up codes
    audit_df["Addr_Lookup_Consignor_Code"] = _normalize_loc_code_series(
        audit_df["Consignor_Combined_Address"].apply(location_finder.extract_from_address)
    )
    audit_df["Addr_Lookup_Consignee_Code"] = _normalize_loc_code_series(
        audit_df["Consignee_Combined_Address"].apply(location_finder.extract_from_address)
    )

    # --- Special override: Mississauga "Suite" consignee rule
    consignee_combo = audit_df["Consignee_Combined_Address"].fillna("").astype(str).str.upper()
    consignor_txt = audit_df["Consignor"].fillna("").astype(str).str.upper().str.strip()

    is_suite_mississauga = consignee_combo.str.startswith("SUITEMISSISSAUGAON")

    consignor_097H = consignor_txt.str.startswith(("LNK", "AMERICAN METAL CRAFTERS", "RADIANS", "EVER READY"))
    consignor_067N = consignor_txt.str.startswith(("VECTAIR", "ZEP"))
    consignor_0897 = consignor_txt.str.startswith(("CHEMFREE", "BERRY GLOBAL"))

    audit_df.loc[is_suite_mississauga & consignor_097H, "Addr_Lookup_Consignee_Code"] = "097H"
    audit_df.loc[is_suite_mississauga & consignor_067N, "Addr_Lookup_Consignee_Code"] = "067N"
    audit_df.loc[is_suite_mississauga & consignor_0897, "Addr_Lookup_Consignee_Code"] = "0897"

    # --- Final location codes
    audit_df["Final Consignor Code"] = (
        clean_blank(audit_df["Extracted Consignor Code"])
        .fillna(clean_blank(audit_df["Org Type Consignor Code"]))
        .fillna(clean_blank(audit_df["Addr_Lookup_Consignor_Code"]))
        .fillna("NON-CINTAS")
    )
    audit_df["Final Consignee Code"] = (
        clean_blank(audit_df["Extracted Consignee Code"])
        .fillna(clean_blank(audit_df["Dest Type Consignee Code"]))
        .fillna(clean_blank(audit_df["Addr_Lookup_Consignee_Code"]))
        .fillna("NON-CINTAS")
    )

    audit_df["Final Consignor Code"] = _normalize_loc_code_series(audit_df["Final Consignor Code"])
    audit_df["Final Consignee Code"] = _normalize_loc_code_series(audit_df["Final Consignee Code"])

    # =========================
    # Exceptions
    # =========================
    EXCEPTION_RULES = [
        {"match_column": "Destination Address", "contains": "6001 W", "set_column": "Final Consignee Code", "value": "0021"},
        {"match_column": "Consignor", "contains": "AVERITT TERMINAL", "set_column": "Final Consignor Code", "value": "0004"},
        {"match_column": "Consignee", "contains": "AVERITT TERMINAL", "set_column": "Final Consignee Code", "value": "0004"},
        {"match_column": "Consignee", "contains": "VALDEZ", "set_column": "Final Consignee Code", "value": "0K35"},
        {"match_column": "Destination Address", "contains": "ATTN: GARDNER", "set_column": "Final Consignee Code", "value": "0536"},
        {"match_column": "Consignor", "contains": "AVERITT TERMINAL", "set_column": "Final Consignor Code", "value": "0004"},
        {"match_column": "Consignor", "contains": "COOPETRAJES", "set_column": "Final Consignor Code", "value": "0896"},
        {"match_column": "Consignee", "contains": "COOPETRAJES", "set_column": "Final Consignee Code", "value": "0896"},
        {"match_column": "Consignor", "contains": "MATHESON", "set_column": "Final Consignor Code", "value": "067N"},
        {"match_column": "Consignor", "contains": "EMPRESSA", "set_column": "Final Consignor Code", "value": "0972"},
        {"match_column": "Consignor", "contains": "EMPRESA", "set_column": "Final Consignor Code", "value": "0972"},
        {"match_column": "Consignee", "contains": "EMPRESSA", "set_column": "Final Consignee Code", "value": "0972"},
        {"match_column": "Consignee", "contains": "EMPRESA", "set_column": "Final Consignee Code", "value": "0972"},
        {"match_column": "Destination Address", "contains": "DEBBIE HUGHES", "set_column": "Final Consignee Code", "value": "0012"},
        {"match_column": "Destination Address", "contains": "LISA SIMPSON", "set_column": "Final Consignee Code", "value": "0210"},
        {"match_column": "Destination Address", "contains": "AMANDA SPEAKS", "set_column": "Final Consignee Code", "value": "0230"},
        {"match_column": "Destination Address", "contains": "ATTN: STOCKROOM", "set_column": "Final Consignee Code", "value": "019M"},
        {"match_column": "Destination Address", "contains": "JOHN VICARI", "set_column": "Final Consignee Code", "value": "0616"},
        {"match_column": "Destination Address", "contains": "AGUEDA", "set_column": "Final Consignee Code", "value": "0464"},
        {"match_column": "Destination Address", "contains": "40 BROADHEAD", "set_column": "Final Consignee Code", "value": "0531"},
        {"match_column": "Destination Address", "contains": "625 ELMWOOD", "set_column": "Final Consignee Code", "value": "0544"},
        {"match_column": "Destination Address", "contains": "TRADE ST", "set_column": "Final Consignee Code", "value": "0312"},
        {"match_column": "Destination Address", "contains": "TRADE STREET", "set_column": "Final Consignee Code", "value": "0312"},
        {"match_column": "Destination Address", "contains": "GWEN ARMSTRONG", "set_column": "Final Consignee Code", "value": "0016"},
        {"match_column": "Destination Address", "contains": "ANNESSA BRITTON", "set_column": "Final Consignee Code", "value": "0172"},
        {"match_column": "Destination Address", "contains": "BRIGHTSEAT", "set_column": "Final Consignee Code", "value": "0041"},
        {"match_column": "Destination Address", "contains": "320 WEST", "set_column": "Final Consignee Code", "value": "0006"},
        {"match_column": "Destination Address", "contains": "1111 N", "set_column": "Final Consignee Code", "value": "0017"},
        {"match_column": "Destination Address", "contains": "1111 NORTHWEST", "set_column": "Final Consignee Code", "value": "0017"},
        {"match_column": "Destination Address", "contains": "1100 REMINGTON", "set_column": "Final Consignee Code", "value": "0022"},
        {"match_column": "Consignee", "contains": "TERRE", "set_column": "Final Consignee Code", "value": "0370"},
    ]

    for rule in EXCEPTION_RULES:
        col = rule["match_column"]
        if col not in audit_df.columns:
            continue
        match_series = (
            audit_df[col]
            .fillna("")
            .astype(str)
            .str.upper()
            .str.contains(rule["contains"], na=False)
        )
        audit_df.loc[match_series, rule["set_column"]] = rule["value"]

    audit_df["Final Consignor Code"] = _normalize_loc_code_series(audit_df["Final Consignor Code"])
    audit_df["Final Consignee Code"] = _normalize_loc_code_series(audit_df["Final Consignee Code"])

    # --- Type code mapping
    if "Loc Code" in master_loc.columns:
        master_loc["Loc Code"] = _normalize_loc_code_series(master_loc["Loc Code"])

    master_code_to_type = dict(
        zip(
            master_loc["Loc Code"].astype(str).str.upper(),
            master_loc["Type Code"].astype(str).str.upper().replace("NAN", pd.NA),
        )
    )

    audit_df["Final Consignor Type"] = (
        audit_df["Final Consignor Code"].astype(str).str.upper().map(master_code_to_type).fillna("NON-CINTAS")
    )
    audit_df["Final Consignee Type"] = (
        audit_df["Final Consignee Code"].astype(str).str.upper().map(master_code_to_type).fillna("NON-CINTAS")
    )

    # --- Responsible party
    mapper = MatrixMapper()
    audit_df["Responsible Party"] = audit_df.apply(mapper.determine_profit_center, axis=1)

    # --- Profit/Cost center lookup
    ml_merge = master_loc.copy()
    if "ProfitCtr" in ml_merge.columns:
       ml_merge["ProfitCtr"] = ml_merge["ProfitCtr"].fillna("").astype(str).str.strip()
    if "Cost Center" in ml_merge.columns:
       ml_merge["Cost Center"] = _as_text_keep_zeros(ml_merge["Cost Center"], decimals=5)
    ml_merge = ml_merge.drop_duplicates(subset=["Loc Code"], keep="first")
    audit_df = audit_df.merge(
       ml_merge[["Loc Code", "ProfitCtr", "Cost Center"]],
       left_on="Responsible Party",
       right_on="Loc Code",
       how="left",
       suffixes=("", "_master"),
    )
    # Rename safely
    if "ProfitCtr" in audit_df.columns:
        audit_df.rename(columns={"ProfitCtr": "Profit Center EJ"}, inplace=True)

    if "Cost Center_master" in audit_df.columns:
        audit_df.rename(columns={"Cost Center_master": "Cost Center EJ"}, inplace=True)
    elif "Cost Center" in audit_df.columns and "Cost Center EJ" not in audit_df.columns:
        audit_df.rename(columns={"Cost Center": "Cost Center EJ"}, inplace=True)

    if "Profit Center" in audit_df.columns:
        audit_df["Profit Center"] = (
            audit_df["Profit Center"]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.replace('"', '', regex=False)
        )

    audit_df["Profit Center EJ"] = audit_df.get(
        "Profit Center EJ", pd.Series([""] * len(audit_df))
    ).fillna("").astype(str).str.strip()

    audit_df["Cost Center EJ"] = _as_text_keep_zeros(
        audit_df.get("Cost Center EJ", pd.Series([""] * len(audit_df))),
        decimals=5,
    )

    # Blank if THIRD PARTY / NON-CINTAS
    rp_norm = audit_df["Responsible Party"].fillna("").astype(str).str.upper().str.strip()
    mask_blank = rp_norm.isin(["THIRD PARTY", "NON-CINTAS"])
    audit_df.loc[mask_blank, "Profit Center EJ"] = ""
    audit_df.loc[mask_blank, "Cost Center EJ"] = ""

    # --- GL account logic
    audit_df["Account # EJ"] = audit_df.apply(
        lambda row: 621000
        if "G59" in str(row.get("Profit Center EJ", ""))
        else (621000 if row.get("Final Consignee Code") == row.get("Responsible Party") else 621020),
        axis=1,
    ).astype(str)

    # --- Automation Accuracy
    if {"Profit Center", "Profit Center EJ"}.issubset(audit_df.columns):
        match = (
            (audit_df["Profit Center"] == audit_df["Profit Center EJ"])
            & audit_df["Profit Center"].notna()
            & audit_df["Profit Center EJ"].notna()
        )
        audit_df["Automation Accuracy"] = match.astype(int)
    else:
        audit_df["Automation Accuracy"] = 0

    # --- Make non-audit rows have same columns
    for col in audit_df.columns:
        if col not in non_audit_df.columns:
            non_audit_df[col] = ""

    for col in non_audit_df.columns:
        if col not in audit_df.columns:
            audit_df[col] = ""

    final_df = pd.concat([audit_df, non_audit_df], axis=0).sort_index()

    # --- Column order
    first_cols = [
        "Profit Center",
        "Cost Center",
        "Account #",
        "Automation Accuracy",
        "Profit Center EJ",
        "Cost Center EJ",
        "Account # EJ",
        "Category",
    ]
    first_cols = [c for c in first_cols if c in final_df.columns]
    other_cols = [c for c in final_df.columns if c not in first_cols]
    final_df = final_df[first_cols + other_cols]

    return final_df
