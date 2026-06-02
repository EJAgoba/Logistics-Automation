from __future__ import annotations
import re
import pandas as pd

def run_error_highlighter(df: pd.DataFrame) -> pd.DataFrame:
   """
   Adds a 'Cost Center Error Check' column to the dataframe based on two rules:
     1. Length rule: the value after '.' in Cost Center must be exactly 5 digits.
     2. Mismatch rule: the value before '.' in Cost Center must match the Profit Center column.
   Fully vectorized — no row-by-row loops.
   """
   cost_col = _find_col(df, [
       "cost center", "costcenter", "cost_center", "costctr", "cost ctr"
   ])
   profit_col = _find_col(df, [
       "profitctr", "profit center", "profit_center", "profitcenter",
       "profit ctr", "profit_ctr", "proft ctr", "profitcentre"
   ])
   if cost_col is None:
       raise ValueError(
           f"Could not find a 'Cost Center' column. Columns found: {list(df.columns)}"
       )
   if profit_col is None:
       raise ValueError(
           f"Could not find a 'Profit Center' / 'ProfitCtr' column. Columns found: {list(df.columns)}"
       )
   # Normalize to clean strings, strip trailing .0 pandas adds to numeric-looking values
   cc = df[cost_col].fillna("").astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
   pc = df[profit_col].fillna("").astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
   # Split on first dot — rows with no dot get empty suffix
   has_dot = cc.str.contains(".", regex=False)
   prefix = cc.where(~has_dot, cc.str.split(".").str[0])
   suffix = cc.where(~has_dot, cc.str.split(".").str[1]).where(has_dot, "")
   # Rule 1: suffix must be exactly 5 digits
   length_err = ~suffix.str.fullmatch(r"\d{5}")
   # Rule 2: prefix must match profit center
   mismatch_err = prefix != pc
   # Build result column
   result = pd.Series("", index=df.index)
   result[length_err & mismatch_err] = "length and mismatch error"
   result[length_err & ~mismatch_err] = "length error"
   result[~length_err & mismatch_err] = "mismatch failure"
   out_df = df.copy()
   out_df["Cost Center Error Check"] = result
   return out_df

def _find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
   """Case-insensitive, whitespace-tolerant column name lookup."""
   cols_lower = {c.lower().strip(): c for c in df.columns}
   for cand in candidates:
       if cand.lower().strip() in cols_lower:
           return cols_lower[cand.lower().strip()]
   return None
