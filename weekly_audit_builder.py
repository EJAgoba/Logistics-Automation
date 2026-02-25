import io

import pandas as pd
from typing import Optional

class WeeklyAuditBuilder:

    """Builds USD/CAD Accounting Summary tabs from edited Weekly Audit workbooks."""

    # === helpers (no header normalization) ===

    @staticmethod

    def _num(series: pd.Series) -> pd.Series:

        s = series.astype(str)

        s = s.str.replace("(", "-", regex=False).str.replace(")", "", regex=False)

        return pd.to_numeric(s, errors="coerce").fillna(0.0).round(2)

    @staticmethod

    def _clean_acct(series: pd.Series) -> pd.Series:

        s = series.astype(str).str.strip()

        s = s.str.replace(r"\.0$", "", regex=True)

        s = s.str.replace(r"\..*$", "", regex=True)

        return s

    def build_currency_sheet(self, df: pd.DataFrame, force_currency: str, selected_run: Optional[str]) -> pd.DataFrame:

        required = [

            "RunNumber", "Profit Center", "Cost Center",

            "Account #", "Currency", "Total Paid Minus Duty and CAD Tax"

        ]

        for col in required:

            if col not in df.columns:

                raise ValueError(f"Edited sheet missing required column: '{col}'")

        paid_col = "Paid" if "Paid" in df.columns else ("Paid Amount" if "Paid Amount" in df.columns else None)

        if not paid_col:

            raise ValueError("Missing 'Paid' or 'Paid Amount' column.")

        if selected_run:

            df = df[df["RunNumber"].astype(str).str.strip() == str(selected_run).strip()]

            if df.empty:

                raise ValueError(f"No rows found for RunNumber {selected_run}")

        base = pd.DataFrame({

            "Run Number":  df["RunNumber"].astype(str).str.strip(),

            "Profit Center": df["Profit Center"].astype(str).str.strip(),

            "Cost Center":   df["Cost Center"].astype(str).str.strip(),

            "Account #":     self._clean_acct(df["Account #"]),

            "Currency":      df["Currency"].astype(str).str.upper().str.strip(),

            "Amount":        self._num(df["Total Paid Minus Duty and CAD Tax"]),

        })

        header_amount = round(-self._num(df[paid_col]).sum(), 2)

        header = {

            "Run Number": str(selected_run or (df["RunNumber"].astype(str).str.strip().iloc[0] if not df.empty else "")),

            "Profit Center": "686",

            "Cost Center": "",

            "Order": "",

            "Account #": "240400",

            "Bus. Area": "",

            "Segment": "",

            "Currency": force_currency,

            "Amount": header_amount,

        }

        tax_specs = [

            ("GST/PST Paid", "GST/PST Account #", "203063"),

            ("HST Paid",     "HST Account #",     "203064"),

            ("QST Paid",     "QST Account #",     "203065"),

            ("Duty Paid",    "Duty Account #",    "621010"),

        ]

        tax_frames = []

        for paid_name, acct_name, default_acct in tax_specs:

            if paid_name in df.columns:

                amt = self._num(df[paid_name])

                mask = amt != 0

                if mask.any():

                    acct_series = self._clean_acct(df[acct_name]) if acct_name in df.columns else pd.Series([""] * len(df), index=df.index)

                    if default_acct is not None:

                        acct_series = acct_series.where(acct_series.replace("", pd.NA).notna(), other=default_acct)

                    tax_frames.append(pd.DataFrame({

                        "Run Number":  df.loc[mask, "RunNumber"].astype(str).str.strip(),

                        "Profit Center": df.loc[mask, "Profit Center"].astype(str).str.strip(),

                        "Cost Center":   df.loc[mask, "Cost Center"].astype(str).str.strip(),

                        "Account #":     self._clean_acct(acct_series.loc[mask]),

                        "Currency":      force_currency,

                        "Amount":        amt.loc[mask].round(2),

                    }))

        combined = pd.concat([base] + tax_frames, ignore_index=True) if tax_frames else base

        grouped = (

            combined.groupby(["Profit Center","Cost Center","Account #","Currency"], dropna=False, as_index=False)["Amount"]

                    .sum()

        )

        grouped["Account #"] = self._clean_acct(grouped["Account #"])

        for c in ["Order","Bus. Area","Segment"]:

            grouped[c] = ""

        out_run = header["Run Number"]

        grouped["Run Number"] = out_run

        grouped["Amount"] = grouped["Amount"].round(2)

        out_df = pd.concat([pd.DataFrame([header]), grouped], ignore_index=True)

        out_df = out_df[["Run Number","Profit Center","Cost Center","Order","Account #","Bus. Area","Segment","Currency","Amount"]]

        out_df["Account #"] = self._clean_acct(out_df["Account #"])

        return out_df

    @staticmethod

    def pack_accounting_summary(usd_df: pd.DataFrame, cad_df: pd.DataFrame) -> bytes:

        """Export USD & CAD sheets with Account # as TEXT (no .0)."""

        bio = io.BytesIO()

        with pd.ExcelWriter(

            bio, engine="xlsxwriter",

            engine_kwargs={"options": {"strings_to_numbers": False}}

        ) as writer:

            usd_df.to_excel(writer, index=False, sheet_name="USD")

            cad_df.to_excel(writer, index=False, sheet_name="CAD")

            wb = writer.book

            text_fmt = wb.add_format({'num_format': '@'})

            for sheet_name, df_out in {"USD": usd_df, "CAD": cad_df}.items():

                ws = writer.sheets[sheet_name]

                acct_idx = df_out.columns.get_loc("Account #")

                ws.set_column(acct_idx, acct_idx, None, text_fmt)

                for r, val in enumerate(df_out["Account #"].astype(str).tolist(), start=1):

                    ws.write_string(r, acct_idx, val)

        bio.seek(0)

        return bio.read()
 