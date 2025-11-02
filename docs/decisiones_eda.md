# Decisiones del EDA

## D1 - Spotify Songs
- Se detectaron 152 valores nulos en la columna "genre" → se reemplazaron por "unknown".
- 34 duplicados por combinación (title + artist) → se eliminaron.
- Outliers en "tempo" (>240 bpm) → se limitaron a 240.

## D2 - Spotify Dashboard
- Diferencias en formato de fechas → se estandarizó con pd.to_datetime.
- Popularidad faltante en 4 registros → se imputó con mediana del dataset.

## D3 - Artist Stats
- Campo "genres" contiene listas → se extrajo el primer género.
- Nulos en "followers" → se rellenaron con 0.

Estas decisiones se aplicaron en las funciones de `transform_d*.py`.
