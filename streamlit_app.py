# -------------------------------
# WEEKLY AUDIT DETAIL BUILDER (SECONDARY)
# -------------------------------
@st.cache_data(show_spinner=False)
def read_weekly_audit(file_bytes: bytes, usd_key: str, cad_key: str):
   xls = pd.ExcelFile(io.BytesIO(file_bytes))
   return pd.read_excel(xls, usd_key), pd.read_excel(xls, cad_key)
st.markdown("#### Weekly Audit Detail Builder (after main automation)")
st.caption("Attach edited Weekly Audit file (must contain 'USD'/'USA' and 'CAD' tabs)")
edited_file = st.file_uploader("Drop your edited Weekly Audit file here", type=["xlsx"], key="edited_wa")
if edited_file is not None:
   try:
       from error_highlighter import run_error_highlighter
       file_key = f"{edited_file.name}_{edited_file.size}"
       xls_names = pd.ExcelFile(edited_file).sheet_names
       names_lower = {s.lower(): s for s in xls_names}
       usd_key = names_lower.get("usd") or names_lower.get("usa")
       cad_key = names_lower.get("cad")
       if not (usd_key and cad_key):
           st.error("Workbook must contain both 'USD' (or 'USA') and 'CAD' sheets.")
       else:
           usd_df_raw, cad_df_raw = read_weekly_audit(edited_file.getvalue(), usd_key, cad_key)
           st.success(f"Edited workbook loaded: USD rows = {len(usd_df_raw):,}, CAD rows = {len(cad_df_raw):,}.")
           run_input = st.text_input(
               "Run Number (required)",
               value="",
               placeholder="e.g. 404",
               key="run_number_input",
           )
           if not run_input.strip():
               st.warning("Enter a run number to continue.")
           else:
               try:
                   run_val = int(run_input.strip())
               except ValueError:
                   run_val = run_input.strip()
               usd_df = usd_df_raw.copy()
               cad_df = cad_df_raw.copy()
               if "RunNumber" in usd_df.columns:
                   usd_df = usd_df[usd_df["RunNumber"] == run_val].reset_index(drop=True)
               if "RunNumber" in cad_df.columns:
                   cad_df = cad_df[cad_df["RunNumber"] == run_val].reset_index(drop=True)
               if len(usd_df) == 0 and len(cad_df) == 0:
                   st.warning(f"No rows found for run number {run_val}. Check the value or column name.")
               else:
                   st.info(f"Filtered to run {run_val} — USD rows: {len(usd_df):,}, CAD rows: {len(cad_df):,}.")
                   # Re-initialize session state only when file or run number changes
                   state_key = f"{file_key}_{run_val}"
                   if st.session_state.get("wa_state_key") != state_key:
                       st.session_state["wa_usd_edited"] = run_error_highlighter(usd_df.copy())
                       st.session_state["wa_cad_edited"] = run_error_highlighter(cad_df.copy())
                       st.session_state["wa_state_key"] = state_key
                   usd_checked = st.session_state["wa_usd_edited"]
                   cad_checked = st.session_state["wa_cad_edited"]
                   usd_errors = (usd_checked["Cost Center Error Check"] != "").sum()
                   cad_errors = (cad_checked["Cost Center Error Check"] != "").sum()
                   total_errors = usd_errors + cad_errors
                   if total_errors > 0:
                       st.error(f"⚠️ {total_errors:,} Cost Center error(s) found — fix them below before building the summary.")
                       with st.form("error_fix_form"):
                           usd_edit = None
                           cad_edit = None
                           usd_flagged_idx = []
                           cad_flagged_idx = []
                           if usd_errors > 0:
                               st.markdown(f"**USD — {usd_errors} error(s)**")
                               usd_flagged_idx = usd_checked[usd_checked["Cost Center Error Check"] != ""].index.tolist()
                               usd_edit = st.data_editor(
                                   usd_checked.loc[usd_flagged_idx, ["Profit Center", "Cost Center", "Cost Center Error Check"]].copy(),
                                   key="usd_editor",
                                   use_container_width=True,
                                   disabled=["Cost Center Error Check"],
                               )
                           if cad_errors > 0:
                               st.markdown(f"**CAD — {cad_errors} error(s)**")
                               cad_flagged_idx = cad_checked[cad_checked["Cost Center Error Check"] != ""].index.tolist()
                               cad_edit = st.data_editor(
                                   cad_checked.loc[cad_flagged_idx, ["Profit Center", "Cost Center", "Cost Center Error Check"]].copy(),
                                   key="cad_editor",
                                   use_container_width=True,
                                   disabled=["Cost Center Error Check"],
                               )
                           submitted = st.form_submit_button("🔄 Re-check & Build")
                       if submitted:
                           if usd_edit is not None:
                               usd_checked.loc[usd_flagged_idx, ["Profit Center", "Cost Center"]] = usd_edit[["Profit Center", "Cost Center"]].values
                           if cad_edit is not None:
                               cad_checked.loc[cad_flagged_idx, ["Profit Center", "Cost Center"]] = cad_edit[["Profit Center", "Cost Center"]].values
                           st.session_state["wa_usd_edited"] = run_error_highlighter(
                               usd_checked.drop(columns=["Cost Center Error Check"])
                           )
                           st.session_state["wa_cad_edited"] = run_error_highlighter(
                               cad_checked.drop(columns=["Cost Center Error Check"])
                           )
                           st.rerun()
                   else:
                       st.success("✅ No Cost Center errors found. Building summary...")
                       builder = WeeklyAuditBuilder()
                       selected_run = None
                       if "RunNumber" in usd_checked.columns and len(usd_checked) > 0:
                           selected_run = usd_checked["RunNumber"].iloc[0]
                       usd_sheet = builder.build_currency_sheet(usd_checked, "USD", selected_run)
                       cad_sheet = builder.build_currency_sheet(cad_checked, "CAD", selected_run)
                       packed = builder.pack_accounting_summary(usd_sheet, cad_sheet)
                       st.download_button(
                           "⬇️ Download Accounting Summary (USD & CAD)",
                           data=packed,
                           file_name=f"Weekly Batch Summary ({run_val}).xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           help="Header = negative of Paid/Paid Amount; details = Total Paid Minus Duty and CAD Tax; Account # is text.",
                       )
   except Exception as e:
       st.error(f"Weekly Audit accounting summary failed: {e}")
