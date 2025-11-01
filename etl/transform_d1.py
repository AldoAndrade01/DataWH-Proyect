import pandas as pd
from .configs import SCHEMA_SONG, CONFIG_D1

def _rename(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    # renombra solo columnas existentes
    exist = {k:v for k,v in mapping.items() if k in df.columns}
    df = df.rename(columns=exist)
    return df

def _normalize_song_df(df: pd.DataFrame, source_tag: str) -> pd.DataFrame:
    df = df.copy()

    # strings básicos
    for c in ["track_id","title","artist","album","genre"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip().str.lower()

    # fechas
    if "release_date" in df.columns:
        df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce")

    # numéricos
    for c in ["duration_ms","popularity","tempo","danceability","energy","valence",
              "loudness","acousticness","speechiness","liveness"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # boolean
    if "explicit" in df.columns:
        df["explicit"] = df["explicit"].astype("boolean")

    # caps simples de negocio
    if "duration_ms" in df.columns:
        df["duration_ms"] = df["duration_ms"].clip(lower=1, upper=15*60*1000)
    if "tempo" in df.columns:
        df["tempo"] = df["tempo"].clip(lower=40, upper=240)

    # duplicados por clave lógica
    df = df.drop_duplicates(subset=[c for c in ["title","artist"] if c in df.columns])

    # tag de origen y subset al esquema
    df["source"] = source_tag
    cols = [c for c in SCHEMA_SONG if c in df.columns]
    return df[cols]

def clean_d1(df: pd.DataFrame, cfg: dict = CONFIG_D1) -> pd.DataFrame:
    df = _rename(df, cfg["mapping"])
    df = _normalize_song_df(df, cfg["source"])
    return df
