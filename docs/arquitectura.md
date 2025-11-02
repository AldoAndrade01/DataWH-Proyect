# Arquitectura del Proyecto

1. **Extracción:** se leen archivos CSV, XLSX y JSON desde /data/raw/.
2. **Transformación:** limpieza y normalización de columnas por fuente (D1, D2, D3).
3. **Carga:** se exportan los datos limpios a /data/interim/.
4. **Unificación:** se realiza un merge entre canciones y artistas y se genera el CSV final en /data/processed/.
5. **API:** expone endpoints REST para cargar y consultar procesos ETL.

Flujo de datos:
[data/raw] → [etl] → [data/interim] → [data/processed] → [api]
