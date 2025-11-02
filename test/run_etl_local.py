from etl.pipeline import run_pipeline_single, run_pipeline_merge

# Rutas de entrada (raw)
d1 = "data/raw/d1_songs_30000.csv"
d2 = "data/raw/d2_spotify_dashboard.xlsm"
d3 = "data/raw/d3_spotify_artist_stats.csv"

# Salidas intermedias
c1 = "data/interim/d1_clean.csv"
c2 = "data/interim/d2_clean.csv"
c3 = "data/interim/d3_artists_clean.csv"

print(">>> Ejecutando D1...")
print(run_pipeline_single("D1", d1, c1))

print(">>> Ejecutando D2...")
print(run_pipeline_single("D2", d2, c2))

print(">>> Ejecutando D3...")
print(run_pipeline_single("D3", d3, c3))

# Merge final a processed
final_out = "data/processed/tracks_clean.csv"
print(">>> Haciendo merge final...")
print(run_pipeline_merge(c1, c2, c3, final_out))
print("Listo. Archivo final:", final_out)
