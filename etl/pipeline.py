from .extract import read_file_into_df
from .transform_d1 import clean_d1
from .transform_d2 import clean_d2
from .transform_d3 import clean_d3
from .merge import merge_canciones_con_artistas
from .load import to_csv

def run_pipeline_single(source_name: str, path_in: str, path_out: str) -> dict:
    df = read_file_into_df(path_in)
    rows_in = len(df)

    if source_name == "D1":
        df = clean_d1(df)
    elif source_name == "D2":
        df = clean_d2(df)
    elif source_name == "D3":
        df = clean_d3(df)
    else:
        raise ValueError("source_name no soportado (usa D1/D2/D3)")

    to_csv(df, path_out)
    return {"rows_in": rows_in, "rows_out": len(df), "output": path_out}

def run_pipeline_merge(path_clean_d1: str, path_clean_d2: str, path_clean_d3_artists: str, out_path: str) -> dict:
    import pandas as pd
    s1 = read_file_into_df(path_clean_d1)
    s2 = read_file_into_df(path_clean_d2)
    a  = read_file_into_df(path_clean_d3_artists)

    # concatenar canciones y enriquecer con artistas
    songs = pd.concat([s1, s2], ignore_index=True)
    merged = merge_canciones_con_artistas(songs, a)
    to_csv(merged, out_path)
    return {"rows_in": len(songs), "rows_out": len(merged), "output": out_path}
