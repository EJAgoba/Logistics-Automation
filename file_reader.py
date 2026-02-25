import os
import pandas as pd

def read_any_file(path: str, sheet_name=0, sep=None) -> pd.DataFrame:
   """
   Reads Excel, CSV, TXT into a pandas DataFrame.
   - Excel: .xlsx, .xls, .xlsm
   - CSV: .csv
   - TXT: .txt (auto sep if not provided)
   """
   if not isinstance(path, str) or path.strip() == "":
       raise ValueError("File path is empty or invalid.")
   ext = os.path.splitext(path)[1].lower()
   # Excel
   if ext in [".xlsx", ".xls", ".xlsm"]:
       return pd.read_excel(path, sheet_name=sheet_name)
   # CSV
   if ext == ".csv":
       return pd.read_csv(path)
   # TXT (tab, pipe, comma, etc.)
   if ext == ".txt":
       # If you know the separator, pass sep="|" or "\t" etc.
       if sep is not None:
           return pd.read_csv(path, sep=sep)
       # Try common separators automatically
       for guess in ["\t", "|", ",", ";"]:
           try:
               df = pd.read_csv(path, sep=guess)
               if df.shape[1] > 1:
                   return df
           except Exception:
               pass
       # Fallback: read as 1 column
       return pd.read_csv(path, sep=None, engine="python")
   raise ValueError(f"Unsupported file type: {ext}")