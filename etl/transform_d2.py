import pandas as pd
from .configs import SCHEMA_SONG, CONFIG_D2
from .transform_d1 import _rename, _normalize_song_df  # reutiliza helpers

def clean_d2(df: pd.DataFrame, cfg: dict = CONFIG_D2) -> pd.DataFrame:
    df = _rename(df, cfg["mapping"])
    df = _normalize_song_df(df, cfg["source"])
    return df
