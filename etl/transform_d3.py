import pandas as pd
from .configs import SCHEMA_ARTIST, CONFIG_D3

def clean_d3(df: pd.DataFrame, cfg: dict = CONFIG_D3) -> pd.DataFrame:
    df = df.copy()
    df = df.rename(columns={k:v for k,v in cfg["mapping"].items() if k in df.columns})

    # normalizar artista
    if "artist" in df.columns:
        df["artist"] = df["artist"].astype(str).str.strip().str.lower()

    # 1er género si viene como lista/cadena separada por coma
    if "primary_genre" in df.columns:
        df["primary_genre"] = (
            df["primary_genre"]
            .astype(str)
            .str.split(",")
            .str[0]
            .str.strip()
            .str.lower()
        )

    # numéricos
    for c in ["followers","artist_popularity","monthly_listeners","world_rank"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # duplicados por artista
    if "artist" in df.columns:
        df = df.drop_duplicates(subset=["artist"])

    # subset esquema
    cols = [c for c in SCHEMA_ARTIST if c in df.columns]
    return df[cols]
