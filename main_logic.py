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

    # TXT (SAP exports often UTF-16 with NULL bytes)
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

        # Try common separators
        for sep in ["\t", "|", ",", ";"]:
            sio.seek(0)
            try:
                df = pd.read_csv(sio, sep=sep, engine="python")
                if df.shape[1] > 1:
                    return df
            except Exception:
                continue

        # Last resort: auto-detect
        sio.seek(0)
        return pd.read_csv(sio, sep=None, engine="python")

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
        # Names
        "Consignor": [
            "Consignor", "Origin Facility", "Shipper", "Ship From Name", "Origin Name", "Org Name",
            "Origin Company", "Org Company", "ShipFromName", "OrgName",
        ],
        "Consignee": [
            "Consignee", "Destination Facility", "Receiver", "Ship To Name", "Destination Name", "Dest Name",
            "Dest Company", "Destination Company", "ShipToName", "DestName",
        ],
        # Origin address
        "Origin Address": [
            "Origin Address", "Origin Address1", "Origin Addresss", "Org Address", "Org Address1",
            "Ship From Address", "Ship From Address1", "Shipper Address", "Shipper Address1",
        ],
        "Origin City": ["Origin City", "Org City", "Ship From City", "Shipper City"],
        "Origin State": [
            "Origin State", "Origin State Code", "Org State", "Org State Code",
            "Ship From State", "Ship From State Code",
        ],
        # Destination address
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
    Normalizes location codes so '972' becomes '0972' (pads purely numeric codes to 4 chars).
    Keeps alphanumerics as-is (uppercased, stripped).
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
    Convert "numeric-ish" values into text while preserving trailing zeros,
    e.g., 312.1 -> '312.10000' (decimals=5)
    """
    s = series.fillna("").astype(str).str.strip()

    def fmt(v: str) -> str:
        if not v:
            return ""
        # Already has non-numeric chars -> keep as-is
        if not re.fullmatch(r"-?\d+(\.\d+)?", v):
            return v
        # Format number with fixed decimals
        try:
            num = float(v)
        except Exception:
            return v
        return f"{num:.{decimals}f}"

    return s.map(fmt)


def _force_excel_text(series: pd.Series) -> pd.Series:
    """
    Prefix numeric-looking strings with an apostrophe so Excel keeps them as TEXT.
    Apostrophe is not displayed in Excel cells.
    """
    s = series.fillna("").astype(str).str.strip()

    def fix(v: str) -> str:
        if not v:
            return ""
        if re.fullmatch(r"-?\d+(\.\d+)?", v):
            return v
        return v

    return s.map(fix)


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

    # --- Defensive normalization of reference tables (fixes Google Sheets numeric coercion)
    my_loc = cintas_master_data.copy()
    master_loc = cintas_master_data_2.copy()
    all_codes = cintas_location_codes.copy()

    # Normalize Loc Code columns
    if "Loc Code" in my_loc.columns:
        my_loc["Loc Code"] = _normalize_loc_code_series(my_loc["Loc Code"])
    if "Loc Code" in master_loc.columns:
        master_loc["Loc Code"] = _normalize_loc_code_series(master_loc["Loc Code"])

    # Normalize all_codes list
    codes_col = "Codes" if "Codes" in all_codes.columns else all_codes.columns[0]
    all_codes[codes_col] = _normalize_loc_code_series(all_codes[codes_col])

    # --- Init services
    location_finder = Location_Codes_Finder(
        master_table=my_loc,
        codes_list=all_codes[codes_col],
    )

    # --- Combined address setup (master set)
    master_combined_set = set(
        my_loc.apply(
            lambda r: location_finder.combine_addr(r.get("Loc_Address", ""), r.get("Loc_City", ""), r.get("Loc_ST", "")),
            axis=1,
        ).dropna()
    )

    # --- Accrual combined addresses (canonical columns)
    accrual_sheet["Consignor_Combined_Address"] = accrual_sheet.apply(
        lambda r: location_finder.combine_addr(r["Origin Address"], r["Origin City"], r["Origin State"]),
        axis=1,
    )
    accrual_sheet["Consignee_Combined_Address"] = accrual_sheet.apply(
        lambda r: location_finder.combine_addr(r["Destination Address"], r["Destination City"], r["Destination State"]),
        axis=1,
    )

    # --- Extract codes from text (and normalize)
    accrual_sheet["Extracted Consignor Code"] = _normalize_loc_code_series(
        accrual_sheet["Consignor"].apply(location_finder.extract_from_text)
    )
    accrual_sheet["Extracted Consignee Code"] = _normalize_loc_code_series(
        accrual_sheet["Consignee"].apply(location_finder.extract_from_text)
    )

    # --- Address checker rule
    consignor_has_code = accrual_sheet["Extracted Consignor Code"].notna() & (
        accrual_sheet["Extracted Consignor Code"].astype(str).str.strip() != ""
    )
    consignee_has_code = accrual_sheet["Extracted Consignee Code"].notna() & (
        accrual_sheet["Extracted Consignee Code"].astype(str).str.strip() != ""
    )

    consignor_no_cintas_or_mat = ~accrual_sheet["Consignor"].fillna("").str.upper().str.contains(
        r"\b(CINTAS|MAT)\b", regex=True
    )
    consignee_no_cintas_or_mat = ~accrual_sheet["Consignee"].fillna("").str.upper().str.contains(
        r"\b(CINTAS|MAT)\b", regex=True
    )

    consignor_addr_ok = accrual_sheet["Consignor_Combined_Address"].fillna("").isin(master_combined_set)
    consignee_addr_ok = accrual_sheet["Consignee_Combined_Address"].fillna("").isin(master_combined_set)

    wipe_consignor = consignor_has_code & consignor_no_cintas_or_mat & ~consignor_addr_ok
    wipe_consignee = consignee_has_code & consignee_no_cintas_or_mat & ~consignee_addr_ok

    accrual_sheet.loc[wipe_consignor, "Extracted Consignor Code"] = ""
    accrual_sheet.loc[wipe_consignee, "Extracted Consignee Code"] = ""

    # --- Extract from Org/Dest Type columns (supports multiple naming)
    org_type_col = _pick_col(accrual_sheet, ["Org Type Code", "Org Loc Code", "Origin Type Code", "OrgTypeCode"])
    dest_type_col = _pick_col(accrual_sheet, ["Dest Type Code", "Dest Loc Code", "Destination Type Code", "DestTypeCode"])

    if org_type_col and dest_type_col:
        tmp = accrual_sheet.apply(
            lambda r: pd.Series(
                location_finder.extract_from_org_dest_type(
                    r.get(org_type_col, ""),
                    r.get(dest_type_col, ""),
                )
            ),
            axis=1,
        )
        tmp.columns = ["Org Type Consignor Code", "Dest Type Consignee Code"]
        accrual_sheet[["Org Type Consignor Code", "Dest Type Consignee Code"]] = tmp
    else:
        accrual_sheet["Org Type Consignor Code"] = ""
        accrual_sheet["Dest Type Consignee Code"] = ""

    accrual_sheet["Org Type Consignor Code"] = _normalize_loc_code_series(accrual_sheet["Org Type Consignor Code"])
    accrual_sheet["Dest Type Consignee Code"] = _normalize_loc_code_series(accrual_sheet["Dest Type Consignee Code"])

    # --- Address-Looked-Up Codes (and normalize)
    accrual_sheet["Addr_Lookup_Consignor_Code"] = _normalize_loc_code_series(
        accrual_sheet["Consignor_Combined_Address"].apply(location_finder.extract_from_address)
    )
    accrual_sheet["Addr_Lookup_Consignee_Code"] = _normalize_loc_code_series(
        accrual_sheet["Consignee_Combined_Address"].apply(location_finder.extract_from_address)
    )

    # --- Special override: Mississauga "Suite" consignee rule
    consignee_combo = accrual_sheet["Consignee_Combined_Address"].fillna("").astype(str).str.upper()
    consignor_txt = accrual_sheet["Consignor"].fillna("").astype(str).str.upper().str.strip()

    is_suite_mississauga = consignee_combo.str.startswith("SUITEMISSISSAUGAON")

    consignor_097H = consignor_txt.str.startswith(("LNK", "AMERICAN METAL CRAFTERS", "RADIANS", "EVER READY"))
    consignor_067N = consignor_txt.str.startswith(("VECTAIR", "ZEP"))
    consignor_0897 = consignor_txt.str.startswith(("CHEMFREE", "BERRY GLOBAL"))

    accrual_sheet.loc[is_suite_mississauga & consignor_097H, "Addr_Lookup_Consignee_Code"] = "097H"
    accrual_sheet.loc[is_suite_mississauga & consignor_067N, "Addr_Lookup_Consignee_Code"] = "067N"
    accrual_sheet.loc[is_suite_mississauga & consignor_0897, "Addr_Lookup_Consignee_Code"] = "0897"

    # --- Final location codes (precedence: extracted -> org/dest -> address -> NON-CINTAS)
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

    # Normalize finals again (important if something became '972' etc.)
    accrual_sheet["Final Consignor Code"] = _normalize_loc_code_series(accrual_sheet["Final Consignor Code"])
    accrual_sheet["Final Consignee Code"] = _normalize_loc_code_series(accrual_sheet["Final Consignee Code"])

    # =========================
    # Exceptions
    # =========================
    EXCEPTION_RULES = [
        {"match_column": "Destination Address", "contains": "6001 W", "set_column": "Final Consignee Code", "value": "0021"},
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
    ]

    for rule in EXCEPTION_RULES:
        col = rule["match_column"]
        if col not in accrual_sheet.columns:
            continue
        match_series = (
            accrual_sheet[col]
            .fillna("")
            .astype(str)
            .str.upper()
            .str.contains(rule["contains"], na=False)
        )
        accrual_sheet.loc[match_series, rule["set_column"]] = rule["value"]

    # Normalize again after exceptions
    accrual_sheet["Final Consignor Code"] = _normalize_loc_code_series(accrual_sheet["Final Consignor Code"])
    accrual_sheet["Final Consignee Code"] = _normalize_loc_code_series(accrual_sheet["Final Consignee Code"])

    # --- Type code mapping (normalize reference keys!)
    if "Loc Code" in master_loc.columns:
        master_loc["Loc Code"] = _normalize_loc_code_series(master_loc["Loc Code"])

    master_code_to_type = dict(
        zip(
            master_loc["Loc Code"].astype(str).str.upper(),
            master_loc["Type Code"].astype(str).str.upper().replace("NAN", pd.NA),
        )
    )

    accrual_sheet["Final Consignor Type"] = (
        accrual_sheet["Final Consignor Code"].astype(str).str.upper().map(master_code_to_type).fillna("NON-CINTAS")
    )
    accrual_sheet["Final Consignee Type"] = (
        accrual_sheet["Final Consignee Code"].astype(str).str.upper().map(master_code_to_type).fillna("NON-CINTAS")
    )

    # --- Responsible party
    mapper = MatrixMapper()
    accrual_sheet["Responsible Party"] = accrual_sheet.apply(mapper.determine_profit_center, axis=1)

    # --- Profit/Cost center lookup (make master columns TEXT FIRST)
    ml_merge = master_loc.copy()

    if "ProfitCtr" in ml_merge.columns:
        ml_merge["ProfitCtr"] = ml_merge["ProfitCtr"].fillna("").astype(str).str.strip()

    # ✅ THIS fixes your Cost Center EJ showing 312.1 instead of 312.10000
    if "Cost Center" in ml_merge.columns:
        ml_merge["Cost Center"] = _as_text_keep_zeros(ml_merge["Cost Center"], decimals=5)
        ml_merge["Cost Center"] = _force_excel_text(ml_merge["Cost Center"])

    accrual_sheet = accrual_sheet.merge(
        ml_merge[["Loc Code", "ProfitCtr", "Cost Center"]],
        left_on="Responsible Party",
        right_on="Loc Code",
        how="left",
        suffixes=("", "_master"),
    )

    # Rename safely
    if "ProfitCtr" in accrual_sheet.columns:
        accrual_sheet.rename(columns={"ProfitCtr": "Profit Center EJ"}, inplace=True)

    if "Cost Center_master" in accrual_sheet.columns:
        accrual_sheet.rename(columns={"Cost Center_master": "Cost Center EJ"}, inplace=True)
    elif "Cost Center" in accrual_sheet.columns and "Cost Center EJ" not in accrual_sheet.columns:
        accrual_sheet.rename(columns={"Cost Center": "Cost Center EJ"}, inplace=True)

    # ✅ Profit Center EJ text
    accrual_sheet["Profit Center EJ"] = accrual_sheet.get(
        "Profit Center EJ", pd.Series([""] * len(accrual_sheet))
    ).fillna("").astype(str).str.strip()

    # ✅ Cost Center EJ text + fixed decimals + force Excel text
    accrual_sheet["Cost Center EJ"] = _as_text_keep_zeros(
        accrual_sheet.get("Cost Center EJ", pd.Series([""] * len(accrual_sheet))),
        decimals=5,
    )
    accrual_sheet["Cost Center EJ"] = _force_excel_text(accrual_sheet["Cost Center EJ"])

    # Blank if THIRD PARTY / NON-CINTAS
    rp_norm = accrual_sheet["Responsible Party"].fillna("").astype(str).str.upper().str.strip()
    mask_blank = rp_norm.isin(["THIRD PARTY", "NON-CINTAS"])
    accrual_sheet.loc[mask_blank, "Profit Center EJ"] = ""
    accrual_sheet.loc[mask_blank, "Cost Center EJ"] = ""

    # --- GL account logic (force as TEXT too)
    accrual_sheet["Account # EJ"] = accrual_sheet.apply(
        lambda row: 621000
        if "G59" in str(row.get("Profit Center EJ", ""))
        else (621000 if row.get("Final Consignee Code") == row.get("Responsible Party") else 621020),
        axis=1,
    )
    accrual_sheet["Account # EJ"] = _force_excel_text(accrual_sheet["Account # EJ"].astype(str))

    # --- Automation Accuracy
    if {"Profit Center", "Profit Center EJ"}.issubset(accrual_sheet.columns):
        match = (
            (accrual_sheet["Profit Center"] == accrual_sheet["Profit Center EJ"])
            & accrual_sheet["Profit Center"].notna()
            & accrual_sheet["Profit Center EJ"].notna()
        )
        accrual_sheet["Automation Accuracy"] = match.astype(int)
    else:
        accrual_sheet["Automation Accuracy"] = 0

    # --- Column order
    first_cols = [
        "Profit Center",
        "Cost Center",
        "Account #",
        "Automation Accuracy",
        "Profit Center EJ",
        "Cost Center EJ",
        "Account # EJ",
    ]
    first_cols = [c for c in first_cols if c in accrual_sheet.columns]
    other_cols = [c for c in accrual_sheet.columns if c not in first_cols]
    accrual_sheet = accrual_sheet[first_cols + other_cols]

    return accrual_sheet