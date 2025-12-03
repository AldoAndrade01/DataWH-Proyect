from fastapi import FastAPI, UploadFile, File, Form, BackgroundTasks, HTTPException
from uuid import uuid4
from pathlib import Path
import json
from typing import Dict, Any, Optional

from etl.pipeline import run_pipeline_single, run_pipeline_merge

app = FastAPI(title="DW Música - ETL API")

STATE_PATH = Path("api/state.json")
STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
if not STATE_PATH.exists():
    STATE_PATH.write_text(json.dumps({}))


# ---------- utilidades de estado ----------
def _read_state() -> Dict[str, Any]:
    return json.loads(STATE_PATH.read_text())

def _write_state(state: Dict[str, Any]) -> None:
    STATE_PATH.write_text(json.dumps(state, indent=2))

def update_state(pid: str, data: Dict[str, Any]) -> None:
    state = _read_state()
    state[pid] = {**state.get(pid, {}), **data}
    _write_state(state)

def get_state(pid: str) -> Optional[Dict[str, Any]]:
    return _read_state().get(pid)


# ---------- endpoints ----------
@app.get("/")
def root():
    return {"message": "API del Data Warehouse de Música lista"}


@app.post("/ingest")
async def ingest(background_tasks: BackgroundTasks,
                 file: UploadFile = File(...),
                 source_name: str = Form(...)):  # D1 | D2 | D3
    source_name = source_name.upper().strip()
    if source_name not in {"D1","D2","D3"}:
        raise HTTPException(400, detail="source_name debe ser D1, D2 o D3")

    pid = str(uuid4())
    raw_dir = Path("data/raw")
    raw_dir.mkdir(parents=True, exist_ok=True)

    raw_path = raw_dir / f"{pid}-{file.filename}"
    raw_path.write_bytes(await file.read())

    update_state(pid, {
        "status": "queued",
        "file": str(raw_path),
        "source": source_name
    })

    out_path = Path("data/interim") / f"{pid}-{source_name}-clean.csv"

    def job():
        try:
            stats = run_pipeline_single(source_name, str(raw_path), str(out_path))
            update_state(pid, {
                "status": "succeeded",
                "stats": stats,
                "cleaned_path": str(out_path)
            })
        except Exception as e:
            update_state(pid, {"status": "failed", "error": str(e)})

    background_tasks.add_task(job)
    return {"process_id": pid}


@app.get("/process/{pid}")
def process_status(pid: str):
    st = get_state(pid)
    if not st:
        raise HTTPException(404, detail="process_id no encontrado")
    return st


@app.get("/catalog")
def catalog():
    processed = [str(p) for p in Path("data/processed").glob("*.csv")]
    interim = [str(p) for p in Path("data/interim").glob("*.csv")]
    return {"processed": processed, "interim": interim}


@app.post("/merge")
def merge_final():
    """
    Usa los tres archivos LIMPIOS estándar:
      data/interim/d1_clean.csv
      data/interim/d2_clean.csv
      data/interim/d3_artists_clean.csv
    y genera: data/processed/tracks_clean.csv
    """
    d1 = Path("data/interim/d1_clean.csv")
    d2 = Path("data/interim/d2_clean.csv")
    d3 = Path("data/interim/d3_artists_clean.csv")

    if not d1.exists() or not d2.exists() or not d3.exists():
        raise HTTPException(400, detail="Faltan limpios en data/interim: d1_clean.csv, d2_clean.csv o d3_artists_clean.csv")

    out = Path("data/processed/tracks_clean.csv")
    out.parent.mkdir(parents=True, exist_ok=True)

    stats = run_pipeline_merge(str(d1), str(d2), str(d3), str(out))
    return {"status": "ok", "stats": stats, "output": str(out)}
