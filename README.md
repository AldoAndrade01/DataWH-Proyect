# 🎵 Data Warehouse de Música  
**Proyecto de la materia: Almacenes de Datos – 7mo semestre**
**Equipo**

**Andrade Ramos Aldo Alberto - 217431633**
**Cano Lopez Brayan Oswaldo - 219423212 **
**Sekaran Rojo Mahatma - 218869276 **

---

## 🧠 Descripción general

Este proyecto implementa un **pipeline ETL modular** (Extracción, Transformación y Carga) con **Pandas** y una **API REST** con **FastAPI**, que permite procesar, limpiar y consolidar datos musicales de Spotify provenientes de tres fuentes distintas.

El resultado final es un **Data Warehouse musical** que genera un dataset limpio y unificado (`tracks_clean.csv`) listo para análisis.

---

## 📁 Estructura del proyecto

```

DataWH-Proyect/
├─ data/
│  ├─ raw/           # Archivos originales (D1, D2, D3)
│  ├─ interim/       # Datos limpios por fuente (ETL intermedio)
│  └─ processed/     # Dataset final consolidado
├─ eda/              # Análisis exploratorio de datos
│  └─ eda.ipynb
├─ etl/              # Código del proceso ETL (modular)
│  ├─ extract.py
│  ├─ transform_d1.py
│  ├─ transform_d2.py
│  ├─ transform_d3.py
│  ├─ merge.py
│  ├─ load.py
│  ├─ pipeline.py
│  └─ configs.py
├─ api/              # API REST con FastAPI
│  └─ main.py
├─ docs/             # Documentación
│  ├─ arquitectura.md
│  ├─ decisiones_eda.md
│  └─ bitacora_etl.md
├─ tests/
│  └─ run_etl_local.py
├─ .venv/            # Entorno virtual (no se sube a Git)
├─ .gitignore
├─ requirements.txt
└─ README.md

````

---

## ⚙️ Instalación paso a paso

### 1️⃣ Clonar el repositorio
```bash
git clone https://github.com/AldoAndrade01/DataWH-Proyect
cd DataWH-Proyect
````

### 2️⃣ Crear entorno virtual

#### En **Windows PowerShell**

```powershell
python -m venv .venv
.\.venv\Scripts\Activate
```

### 3️⃣ Instalar dependencias

```bash
pip install -r requirements.txt
```

### 4️⃣ Revisar carpetas de datos

Confirma que existan:

```
data/raw/         → aquí van los datasets originales (D1, D2, D3)
data/interim/     → se generan automáticamente (limpios)
data/processed/   → se genera automáticamente (final consolidado)
```

---

## 🎧 Datasets utilizados

| Clave  | Nombre del dataset            | Formato | Descripción                                  |
| ------ | ----------------------------- | ------- | -------------------------------------------- |
| **D1** | `d1_songs_30000.csv`          | CSV     | 30,000 canciones con métricas de Spotify     |
| **D2** | `d2_spotify_dashboard.xlsm`   | Excel   | Dataset limpio para análisis (formato Excel) |
| **D3** | `d3_spotify_artist_stats.csv` | CSV     | Estadísticas y metadatos de artistas         |

Coloca los tres archivos dentro de la carpeta:

```
data/raw/
```

---

## 🧩 Ejecución del ETL (modo local)

1️⃣ Asegúrate de que tu entorno virtual esté activo.
2️⃣ Ejecuta el script de prueba:

```bash
python tests/run_etl_local.py
```

Esto ejecutará automáticamente las tres fases del pipeline:

* Limpieza y transformación de D1, D2 y D3 → genera tres archivos en `data/interim/`
* Unión final (merge) → genera `tracks_clean.csv` en `data/processed/`

**Resultado esperado:**

```
data/interim/d1_clean.csv
data/interim/d2_clean.csv
data/interim/d3_artists_clean.csv
data/processed/tracks_clean.csv
```

---

## 🌐 Ejecución del API (modo FastAPI)

### 1️⃣ Levantar el servidor

```bash
uvicorn api.main:app --reload
```

### 2️⃣ Probar que la API funciona

Abre el navegador en:
👉 [http://127.0.0.1:8000](http://127.0.0.1:8000)
Deberías ver:

```json
{"message": "API
```
