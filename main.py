import os
import uuid
import threading
import sqlite3
import traceback
from datetime import datetime
from typing import List, Optional

import aiofiles
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
import pandas as pd

APP_DIR = os.path.dirname(__file__)
UPLOAD_DIR = os.path.join(APP_DIR, "uploads")
CLEAN_DIR = os.path.join(APP_DIR, "cleaned")
DB_PATH = os.path.join(APP_DIR, "etl_status.db")

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(CLEAN_DIR, exist_ok=True)

#Logica
def init_db():
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    try:
        c = conn.cursor()
        c.execute("""
        CREATE TABLE IF NOT EXISTS processes (
            id TEXT PRIMARY KEY,
            created_at TEXT,
            status TEXT,
            progress INTEGER,
            errors TEXT,
            stats TEXT,
            cleaned_path TEXT
        )
        """)
        conn.commit()
    finally:
        conn.close()

init_db()

def upsert_process(pid, **kwargs):
    with sqlite3.connect(DB_PATH, timeout=10.0) as conn:
        c = conn.cursor()

        c.execute("SELECT id FROM processes WHERE id = ?", (pid,))
        exists = c.fetchone()

        if "stats" in kwargs and isinstance(kwargs["stats"], dict):
            import json
            kwargs["stats"] = json.dumps(kwargs["stats"])

        if exists:
            sets = []
            vals = []
            for k, v in kwargs.items():
                sets.append(f"{k} = ?")
                vals.append(v)
            vals.append(pid)
            qry = f"UPDATE processes SET {', '.join(sets)} WHERE id = ?"
            c.execute(qry, vals)
        else:
            columns = ["id", "created_at", "status", "progress", "errors", "stats", "cleaned_path"]
            values = [pid, datetime.now().isoformat(), kwargs.get("status", "queued"), kwargs.get("progress", 0),
                      kwargs.get("errors", ""), kwargs.get("stats", ""), kwargs.get("cleaned_path", "")]
            c.execute(f"INSERT INTO processes ({','.join(columns)}) VALUES ({','.join(['?'] * len(columns))})", values)

        conn.commit()


def get_process(pid):
    import json

    with sqlite3.connect(DB_PATH, timeout=10.0) as conn:
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("SELECT id, created_at, status, progress, errors, stats, cleaned_path FROM processes WHERE id = ?",
                  (pid,))
        row = c.fetchone()

        if not row:
            return None

        stats_data = row["stats"] or "{}"
        try:
            stats_dict = json.loads(stats_data)
        except json.JSONDecodeError:
            stats_dict = stats_data

        return {
            "id": row["id"],
            "created_at": row["created_at"],
            "status": row["status"],
            "progress": int(row["progress"]),
            "errors": row["errors"] or "",
            "stats": stats_dict,
            "cleaned_path": row["cleaned_path"] or "",
        }

#API
app = FastAPI()

class StartResponse(BaseModel):
    process_id: str

class StatusResponse(BaseModel):
    id: str
    created_at: str
    status: str
    progress: int
    errors: Optional[str]
    stats: Optional[str]
    cleaned_path: Optional[str]

def read_file_into_df(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext in [".csv"]:
        df = pd.read_csv(path, low_memory=False)
    elif ext in [".xlsx", ".xls"]:
        df = pd.read_excel(path)
    elif ext in [".json"]:
        df = pd.read_json(path, lines=False)
    else:

        df = pd.read_csv(path, low_memory=False)
    return df

def simple_inspect(df: pd.DataFrame):
    info = {}
    info["rows"] = int(df.shape[0])
    info["cols"] = int(df.shape[1])
    info["dtypes"] = df.dtypes.astype(str).to_dict()
    return info

def detect_anomalies_and_stats(df: pd.DataFrame):
    stats = {}
    stats["missing_per_column"] = df.isna().sum().to_dict()
    stats["duplicates"] = int(df.duplicated().sum())
    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
    outliers = {}
    for col in numeric_cols:
        series = df[col].dropna()
        if series.empty:
            continue
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        outlier_count = int(((series < lower) | (series > upper)).sum())
        outliers[col] = {"outlier_count": outlier_count, "q1": float(q1), "q3": float(q3)}
    stats["outliers"] = outliers
    return stats

def transform_df(df: pd.DataFrame):
    df = df.copy()
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]

    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).str.strip().replace({"nan": None})

    for col in df.columns:
        if "date" in col or "fecha" in col:
            try:
                df[col] = pd.to_datetime8(df[col], errors="coerce")
            except Exception:
                pass

    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            median = df[col].median(skipna=True)
            if pd.isna(median):
                median = 0
            df[col] = df[col].fillna(median)
        else:
            mode = None
            try:
                mode = df[col].mode(dropna=True)
                if len(mode) > 0:
                    mode = mode.iloc[0]
            except Exception:
                mode = None
            if mode is None:
                df[col] = df[col].fillna("unknown")
            else:
                df[col] = df[col].fillna(mode)

    before = df.shape[0]
    df = df.drop_duplicates()
    after = df.shape[0]
    transformed = {
        "rows_before": int(before),
        "rows_after": int(after),
        "rows_removed_duplicates": int(before - after)
    }
    return df, transformed

def run_etl_process(pid: str, file_paths: List[str], source_name: Optional[str] = None):
    try:
        upsert_process(pid, status="running", progress=0, errors="", stats="")
        aggregate_cleaned = []
        overall_stats = {"files": {}}
        total_files = len(file_paths)
        for idx, path in enumerate(file_paths, start=1):
            fname = os.path.basename(path)
            file_stats = {"file_name": fname}
            try:
                df = read_file_into_df(path)
                file_stats["initial_rows"] = int(df.shape[0])
                file_stats["initial_cols"] = int(df.shape[1])

                if df.shape[0] < 1000 or df.shape[1] < 10:
                    file_stats["warning"] = "Less than required rows/columns (>=1000 rows and >=10 cols recommended)."

                file_stats["inspect"] = simple_inspect(df)

                file_stats["anomalies"] = detect_anomalies_and_stats(df)

                clean_df, transform_info = transform_df(df)
                file_stats["transform_info"] = transform_info

                file_stats["cleaned_rows"] = int(clean_df.shape[0])
                file_stats["cleaned_cols"] = int(clean_df.shape[1])

                aggregate_cleaned.append(clean_df)
                overall_stats["files"][fname] = file_stats
            except Exception as e:
                tb = traceback.format_exc()
                file_stats["error"] = str(e)
                file_stats["traceback"] = tb

                prev_errors = get_process(pid)["errors"] or ""
                new_errors = prev_errors + f"\nError processing {fname}: {str(e)}"
                upsert_process(pid, errors=new_errors)

            progress = int((idx / total_files) * 90)
            upsert_process(pid, progress=progress)

        if aggregate_cleaned:
            try:
                combined = pd.concat(aggregate_cleaned, ignore_index=True, sort=False)
            except ValueError:
                combined = aggregate_cleaned[0]

            before = combined.shape[0]
            combined = combined.drop_duplicates()
            after = combined.shape[0]
            overall_stats["merged"] = {
                "merged_rows_before": int(before),
                "merged_rows_after": int(after),
                "merged_rows_removed": int(before - after)
            }

            cleaned_filename = f"cleaned_{pid}.csv"
            cleaned_path = os.path.join(CLEAN_DIR, cleaned_filename)
            combined.to_csv(cleaned_path, index=False, encoding="utf-8")
            upsert_process(pid, cleaned_path=cleaned_path)
            overall_stats["cleaned_file"] = cleaned_filename
        else:
            overall_stats["warning"] = "No se ha limpiando el archivo."


        upsert_process(pid, status="finished", progress=100, stats=str(overall_stats))
    except Exception as e:
        tb = traceback.format_exc()
        upsert_process(pid, status="failed", errors=tb, progress=0)

#EndPoints
@app.get("/")
def index():
    return {"Hello": "World"}

@app.post("/etl/start", response_model=StartResponse)
async def start_etl(
    files: List[UploadFile] = File(...),
    source_name: Optional[str] = Form(None)
):
    if not files or len(files) == 0:
        raise HTTPException(status_code=400, detail="Se debe enviar al menos un archivo.")

    # Save uploaded files to disk
    saved_paths = []
    for f in files:
        extension = os.path.splitext(f.filename)[1].lower()
        uid = uuid.uuid4().hex
        safe_name = f"{uid}_{f.filename}"
        save_path = os.path.join(UPLOAD_DIR, safe_name)
        async with aiofiles.open(save_path, "wb") as out_file:
            content = await f.read()
            await out_file.write(content)
        saved_paths.append(save_path)

    pid = uuid.uuid4().hex
    upsert_process(pid, status="queued", progress=0, errors="", stats="")
    # Launch background thread to run ETL
    t = threading.Thread(target=run_etl_process, args=(pid, saved_paths, source_name), daemon=True)
    t.start()
    return {"process_id": pid}

@app.get("/etl/status/{pid}", response_model=StatusResponse)
def etl_status(pid: str):
    p = get_process(pid)
    if not p:
        raise HTTPException(status_code=404, detail="Process ID no encontrado")
    # expose cleaned_path as just filename for convenience
    cleaned_path = p.get("cleaned_path") or ""
    return StatusResponse(
        id=p["id"],
        created_at=p["created_at"],
        status=p["status"],
        progress=p["progress"],
        errors=p["errors"],
        stats=p["stats"],
        cleaned_path=cleaned_path
    )

@app.get("/etl/download/{pid}")
def download_cleaned(pid: str):
    p = get_process(pid)
    if not p:
        raise HTTPException(status_code=404, detail="Process ID no encontrado")
    cleaned_path = p.get("cleaned_path")
    if not cleaned_path or not os.path.exists(cleaned_path):
        raise HTTPException(status_code=404, detail="Archivo limpio no disponible")
    return FileResponse(cleaned_path, filename=os.path.basename(cleaned_path), media_type="text/csv")

@app.on_event("shutdown")
def close_db_connection():
    global _db_conn
    if _db_conn:
        _db_conn.close()