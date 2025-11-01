from pathlib import Path

def to_csv(df, path_out: str):
    Path(path_out).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path_out, index=False, encoding="utf-8")
    return path_out
