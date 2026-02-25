from __future__ import annotations
import io
import streamlit as st
import pandas as pd
from ref_store import RefSheets, load_refs, append_row, find_codes_column, get_pending_rows_df, clear_pending_rows
from main_logic import run_pipeline, read_uploaded_to_df
from weekly_audit_builder import WeeklyAuditBuilder

st.set_page_config(page_title="Logistics Financials Automation", layout="wide")
st.title("Logistics Financials Automation")
# ----------------------------
# Google Sheets Ref Config
# ----------------------------
REF_SHEETS = RefSheets.from_env_or_defaults()
# ----------------------------
# Cached load refs (read-only public)
# ----------------------------
@st.cache_data(show_spinner=False)
def cached_load_refs():
   return load_refs(REF_SHEETS, ttl=600)

def clear_refs_cache():
   st.cache_data.clear()

# -------------------------------
# Weekly Audit Builder helpers
# -------------------------------
def _pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
   cols_lower = {c.lower(): c for c in df.columns}
   for cand in candidates:
       if cand.lower() in cols_lower:
           return cols_lower[cand.lower()]
   return None

def _clean_str_series(s: pd.Series) -> pd.Series:
   return s.fillna("").astype(str).str.strip()

def safe_read_uploaded(uploaded) -> pd.DataFrame:
   """
   Uses your existing read_uploaded_to_df(uploaded),
   but removes NULL bytes for txt/csv uploads first (fixes pandas ParserError).
   """
   name = (uploaded.name or "").lower()
   if name.endswith(".txt") or name.endswith(".csv"):
       raw = uploaded.getvalue()
       if b"\x00" in raw:
           raw = raw.replace(b"\x00", b"")
       cleaned = io.BytesIO(raw)
       class _Up:
           def __init__(self, b, name):
               self._b = b
               self.name = name
           def getvalue(self):
               return self._b.getvalue()
       return read_uploaded_to_df(_Up(cleaned, uploaded.name))
   return read_uploaded_to_df(uploaded)

# ----------------------------
# Load refs (from Google Sheets)
# ----------------------------
refs = cached_load_refs()
# ----------------------------
# Sidebar: Reference Tables (overlay edits)
# ----------------------------
with st.sidebar:
   st.header("Reference Tables")
   st.caption(
       "Reference tables are read from PUBLIC Google Sheets (read-only). "
       "Edits here apply as a temporary overlay for this session/run. "
       "To permanently update the sheet, download the pending rows and paste them into the Google Sheet."
   )
   st.write("Spreadsheet:", REF_SHEETS.spreadsheet_url)
   with st.expander("➕ Add to MY LOCATION TABLE", expanded=False):
       st.write("Used for combined address matching and address-based lookup.")
       loc_code = st.text_input("Loc Code", value="")
       addr = st.text_input("Loc_Address", value="")
       city = st.text_input("Loc_City", value="")
       state = st.text_input("Loc_ST", value="")
       zip_code = st.text_input("Zip_Code", value="")
       country = st.text_input("C_C", value="")
       type_code = st.text_input("Type_Code", value="")
       profit_center = st.text_input("ProfitCtr", value="")
       cost_center = st.text_input("Cost Center", value="")
       if st.button("Add row (session overlay) → MY LOCATION TABLE (4)"):
           try:
               row = {
                   "Loc_Address": addr,
                   "Loc_City": city,
                   "Loc_ST": state,
                   "Zip_Code": zip_code,
                   "C_C": country,
                   "Type_Code": type_code,
                   "ProfitCtr": profit_center,
                   "Cost Center": cost_center,
               }
               if loc_code.strip():
                   row["Loc Code"] = loc_code.strip()
               refs = append_row(refs, "my_location", row)
               st.success("Row added to session overlay.")
           except Exception as e:
               st.error(str(e))
       pending = get_pending_rows_df("my_location")
       if len(pending) > 0:
           st.caption(f"Pending rows (MY LOCATION TABLE): {len(pending)}")
           st.download_button(
               "⬇ Download pending MY LOCATION rows (CSV)",
               data=pending.to_csv(index=False).encode("utf-8"),
               file_name="pending_my_location_rows.csv",
               mime="text/csv",
           )
           if st.button("Clear pending MY LOCATION rows"):
               clear_pending_rows("my_location")
               st.success("Cleared.")
               st.rerun()
   with st.expander("➕ Add to Master Location Table", expanded=False):
       st.write("Used for Type Code + ProfitCtr + Cost Center lookups.")
       loc_code = st.text_input("Loc Code", value="", key="ml_loc_code")
       type_code = st.text_input("Type Code", value="", key="ml_type_code")
       profit = st.text_input("ProfitCtr", value="", key="ml_profit")
       cost = st.text_input("Cost Center", value="", key="ml_cost")
       if st.button("Add row (session overlay) → Master Location Table"):
           try:
               row = {
                   "Loc Code": loc_code.strip(),
                   "Type Code": type_code.strip(),
                   "ProfitCtr": profit.strip(),
                   "Cost Center": cost.strip(),
               }
               refs = append_row(refs, "master_location", row)
               st.success("Row added to session overlay.")
           except Exception as e:
               st.error(str(e))
       pending = get_pending_rows_df("master_location")
       if len(pending) > 0:
           st.caption(f"Pending rows (Master Location): {len(pending)}")
           st.download_button(
               "⬇ Download pending Master Location rows (CSV)",
               data=pending.to_csv(index=False).encode("utf-8"),
               file_name="pending_master_location_rows.csv",
               mime="text/csv",
           )
           if st.button("Clear pending Master Location rows"):
               clear_pending_rows("master_location")
               st.success("Cleared.")
               st.rerun()
   with st.expander("➕ Add to Location Codes List", expanded=False):
       st.write("Used to validate/extract allowed codes.")
       codes_col = find_codes_column(refs["all_codes"])
       new_code = st.text_input(f"New code ({codes_col})", value="")
       if st.button("Add code (session overlay)"):
           try:
               refs = append_row(refs, "all_codes", {codes_col: new_code.strip()})
               st.success("Code added to session overlay.")
           except Exception as e:
               st.error(str(e))
       pending = get_pending_rows_df("all_codes")
       if len(pending) > 0:
           st.caption(f"Pending codes: {len(pending)}")
           st.download_button(
               "⬇ Download pending Codes rows (CSV)",
               data=pending.to_csv(index=False).encode("utf-8"),
               file_name="pending_all_codes_rows.csv",
               mime="text/csv",
           )
           if st.button("Clear pending Codes rows"):
               clear_pending_rows("all_codes")
               st.success("Cleared.")
               st.rerun()
   st.divider()
   st.subheader("Quick Peek")
   st.write("MY LOCATION TABLE rows:", len(refs["my_location"]))
   st.write("Master Location Table rows:", len(refs["master_location"]))
   st.write("All Location Codes rows:", len(refs["all_codes"]))
   if st.button("🔄 Refresh refs from Google Sheets"):
       clear_refs_cache()
       st.success("Cache cleared — reload triggered.")
       st.rerun()

# -------------------------------
# MAIN AUTOMATION (PRIMARY)
# -------------------------------
st.subheader("Main Automation")
uploaded_main = st.file_uploader(
   "Upload Financial Data - Accruals or Weekly Batch Reports (.xlsx, .csv, .txt)",
   type=["xlsx", "xls", "xlsm", "csv", "txt"],
   key="uploader_main",
)
col1, col2 = st.columns([1, 1], gap="large")
with col1:
   run_main = st.button("▶ Run Main Automation", disabled=(uploaded_main is None))
with col2:
   st.info("Output will download as Excel after the pipeline finishes.")
if run_main and uploaded_main is not None:
   with st.spinner("Reading file..."):
       accrual_df = safe_read_uploaded(uploaded_main)
   with st.spinner("Running pipeline..."):
       out_df = run_pipeline(
           accrual_sheet=accrual_df,
           cintas_master_data=refs["my_location"],
           cintas_master_data_2=refs["master_location"],
           cintas_location_codes=refs["all_codes"],
       )
   st.success("Done.")
   st.dataframe(out_df.head(50), use_container_width=True)
   buffer = io.BytesIO()
   out_df.to_excel(buffer, index=False)
   buffer.seek(0)
   st.download_button(
       label="⬇ Download Output Excel",
       data=buffer,
       file_name="Weekly_Batch_Output.xlsx",
       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
   )

# -------------------------------
# WEEKLY AUDIT DETAIL BUILDER (SECONDARY)
# -------------------------------
st.markdown("#### Weekly Audit Detail Builder (after main automation)")
st.caption("Attach edited Weekly Audit file (must contain 'USD'/'USA' and 'CAD' tabs)")
edited_file = st.file_uploader("Drop your edited Weekly Audit file here", type=["xlsx"], key="edited_wa")
if edited_file is not None:
   try:
       xls = pd.ExcelFile(edited_file)
       names_lower = {s.lower(): s for s in xls.sheet_names}
       usd_key = names_lower.get("usd") or names_lower.get("usa")
       cad_key = names_lower.get("cad")
       if not (usd_key and cad_key):
           st.error("Workbook must contain both 'USD' (or 'USA') and 'CAD' sheets.")
       else:
           usd_df = pd.read_excel(xls, usd_key)
           cad_df = pd.read_excel(xls, cad_key)
           st.success(f"Edited workbook loaded: USD rows = {len(usd_df):,}, CAD rows = {len(cad_df):,}.")
           builder = WeeklyAuditBuilder()
           selected_run = None
           if "RunNumber" in usd_df.columns and len(usd_df) > 0:
               selected_run = usd_df["RunNumber"].iloc[0]
           usd_sheet = builder.build_currency_sheet(usd_df, "USD", selected_run)
           cad_sheet = builder.build_currency_sheet(cad_df, "CAD", selected_run)
           packed = builder.pack_accounting_summary(usd_sheet, cad_sheet)
           st.download_button(
               "⬇️ Download Accounting Summary (USD & CAD)",
               data=packed,
               file_name=f"Accounting Summary (Run {selected_run or 'auto'}).xlsx",
               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
               help="Header = negative of Paid/Paid Amount; details = Total Paid Minus Duty and CAD Tax; Account # is text.",
           )
   except Exception as e:
       st.error(f"Weekly Audit accounting summary failed: {e}")