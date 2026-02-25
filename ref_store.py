from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any, Optional
import pandas as pd
import streamlit as st
from streamlit_gsheets import GSheetsConnection

# ----------------------------
# CONFIG
# ----------------------------
@dataclass(frozen=True)
class RefSheets:
   """
   Points to your public Google Sheets workbook + tab names.
   spreadsheet_url: full URL to the Google Sheet
   my_location_ws: tab name for MY LOCATION TABLE
   master_location_ws: tab name for Master Location Table
   all_codes_ws: tab name for all_location_codes
   """
   spreadsheet_url: str
   my_location_ws: str
   master_location_ws: str
   all_codes_ws: str
   @staticmethod
   def from_env_or_defaults() -> "RefSheets":
       """
       Optional: you can hardcode the URL + tabs, or later load them from env vars.
       For now: fill these in.
       """
       return RefSheets(
           spreadsheet_url="https://docs.google.com/spreadsheets/d/1r26helJRHHmgOtfOIt5FK-KVQRPADLVtnQUVDZBzTfc/edit?",
           my_location_ws="0",
           master_location_ws="36761169",
           all_codes_ws="1227706288",
       )

# ----------------------------
# CONNECTION
# ----------------------------
def get_gsheets_conn() -> GSheetsConnection:
   """
   Streamlit connection (no secrets required for public sheets).
   """
   return st.connection("gsheets", type=GSheetsConnection)

# ----------------------------
# LOADING REFS
# ----------------------------
def load_refs(sheets: RefSheets, ttl: int = 60) -> Dict[str, pd.DataFrame]:
   """
   Reads reference tabs from a PUBLIC Google Sheet.
   No secrets required for reading.
   """
   conn = get_gsheets_conn()
   my_location = conn.read(spreadsheet=sheets.spreadsheet_url, worksheet=sheets.my_location_ws, ttl=ttl)
   master_location = conn.read(spreadsheet=sheets.spreadsheet_url, worksheet=sheets.master_location_ws, ttl=ttl)
   all_codes = conn.read(spreadsheet=sheets.spreadsheet_url, worksheet=sheets.all_codes_ws, ttl=ttl)
   # Ensure DataFrames
   my_location = pd.DataFrame(my_location)
   master_location = pd.DataFrame(master_location)
   all_codes = pd.DataFrame(all_codes)
   return {
       "my_location": my_location,
       "master_location": master_location,
       "all_codes": all_codes,
   }

# ----------------------------
# APPEND ROW (NO-SECRETS MODE)
# ----------------------------
def _ensure_pending_store():
   if "pending_ref_changes" not in st.session_state:
       st.session_state["pending_ref_changes"] = {
           "my_location": [],
           "master_location": [],
           "all_codes": [],
       }

def append_row(
   refs: Dict[str, pd.DataFrame],
   key: str,
   row: Dict[str, Any],
   persist_to_google: bool = False,
) -> Dict[str, pd.DataFrame]:
   """
   Appends a row IN-MEMORY (session overlay). This works with ZERO secrets.
   If you ever want to persist to Google Sheets directly, that will require OAuth/service account.
   persist_to_google is kept here as a future flag, but it is NOT implemented in no-secrets mode.
   """
   if persist_to_google:
       raise RuntimeError(
           "Persisting edits back to Google Sheets requires authentication (secrets/OAuth). "
           "Right now you are in no-secrets mode."
       )
   if key not in refs:
       raise ValueError(f"Unknown reference key: {key}")
   _ensure_pending_store()
   st.session_state["pending_ref_changes"][key].append(row)
   # Apply overlay to current refs immediately
   df = refs[key].copy()
   # Add missing columns
   for col in row.keys():
       if col not in df.columns:
           df[col] = pd.NA
   new_row = {col: row.get(col, pd.NA) for col in df.columns}
   df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
   updated = dict(refs)
   updated[key] = df
   return updated

def get_pending_rows_df(key: str) -> pd.DataFrame:
   _ensure_pending_store()
   rows = st.session_state["pending_ref_changes"].get(key, [])
   return pd.DataFrame(rows)

def clear_pending_rows(key: Optional[str] = None) -> None:
   _ensure_pending_store()
   if key is None:
       st.session_state["pending_ref_changes"] = {
           "my_location": [],
           "master_location": [],
           "all_codes": [],
       }
   else:
       st.session_state["pending_ref_changes"][key] = []

# ----------------------------
# HELPERS
# ----------------------------
def find_codes_column(codes_df: pd.DataFrame) -> str:
   preferred = ["Codes", "Code", "LOC_CODE", "Loc Code", "loc_code"]
   for c in preferred:
       if c in codes_df.columns:
           return c
   return codes_df.columns[0]