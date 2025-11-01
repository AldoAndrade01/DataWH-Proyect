import os
import pandas as pd

def read_file_into_df(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext == ".csv":
        return pd.read_csv(path, low_memory=False)
    if ext in (".xlsx", ".xls", ".xlsm"):
        return pd.read_excel(path)
    if ext == ".json":
        try:
            return pd.read_json(path, lines=False)
        except ValueError:
            return pd.read_json(path, lines=True)
    # fallback
    return pd.read_csv(path, low_memory=False)
