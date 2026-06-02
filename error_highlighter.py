from __future__ import annotations
import re
import pandas as pd

def run_error_highlighter(df: pd.DataFrame) -> pd.DataFrame:
   """
   Adds a 'Cost Center Error Check' column to the dataframe based on two rules:
     1. Length rule: the value after '.' in Cost Center must be exactly 5 digits.
     2. Mismatch rule: the value before '.' in Cost Center must match the Profit Center column.
   Output values:
     - ""                        → no errors
     - "length error"            → digit count after '.' is not exactly 5
     - "mismatch failure"        → prefix before '.' does not match Profit Center
     - "length and mismatch error" → both rules fail
   """
   cost_col = _find_col(df, ["cost center", "costcenter", "cost_center"])
   profit_col = _find_col(df, ["profitctr", "profit center", "profit_center", "profitcenter"])
   if cost_col is None:
       raise ValueError("Could not find a 'Cost Center' column in the uploaded file.")
   if profit_col is None:
       raise ValueError("Could not find a 'Profit Center' / 'ProfitCtr' column in the uploaded file.")
   results = []
   for _, row in df.iterrows():
       cc_raw = str(row[cost_col]).strip() if pd.notna(row[cost_col]) else ""
       pc_raw = str(row[profit_col]).strip() if pd.notna(row[profit_col]) else ""
       length_err = False
       mismatch_err = False
       if "." in cc_raw:
           prefix, suffix = cc_raw.split(".", 1)
       else:
           # No dot at all — treat as both errors
           prefix = cc_raw
           suffix = ""
       # Rule 1: exactly 5 digits after the dot
       if not re.fullmatch(r"\d{5}", suffix):
           length_err = True
       # Rule 2: prefix before dot must match Profit Center
       if prefix != pc_raw:
           mismatch_err = True
       if length_err and mismatch_err:
           results.append("length and mismatch error")
       elif length_err:
           results.append("length error")
       elif mismatch_err:
           results.append("mismatch failure")
       else:
           results.append("")
   out_df = df.copy()
   out_df["Cost Center Error Check"] = results
   return out_df

def _find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
   """Case-insensitive column name lookup."""
   cols_lower = {c.lower().strip(): c for c in df.columns}
   for cand in candidates:
       if cand.lower() in cols_lower:
           return cols_lower[cand.lower()]
   return None
