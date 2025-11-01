import pandas as pd

def merge_canciones_con_artistas(df_songs: pd.DataFrame, df_art: pd.DataFrame) -> pd.DataFrame:
    if "artist" in df_songs.columns and "artist" in df_art.columns:
        return df_songs.merge(df_art, on="artist", how="left", suffixes=("","_artist"))
    return df_songs
