# ml/training.py
from pathlib import Path
import matplotlib.pyplot as plt

PLOTS_DIR = Path("api/static/plots")
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

def _crear_grafica_dummy(nombre_archivo: str, titulo: str) -> str:
    """Crea una gráfica muy simple solo para probar el flujo."""
    fig, ax = plt.subplots()
    ax.plot([1, 2, 3], [2, 3, 5])
    ax.set_title(titulo)
    out_path = PLOTS_DIR / nombre_archivo
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    # Devolvemos la ruta relativa desde /static
    return f"plots/{nombre_archivo}"

def run_training_and_plots(tracks_path: str) -> dict:
    """
    Versión de prueba. 
    Aqui te la tienes que rifar tu cano.
    """

    # Métricas ficticias de ejemplo
    metrics = [
        {"modelo": "LinearRegression", "MAE": 5.12, "RMSE": 6.80, "R2": 0.78},
        {"modelo": "KNNRegressor",    "MAE": 4.80, "RMSE": 6.30, "R2": 0.81},
        {"modelo": "RandomForest",    "MAE": 4.10, "RMSE": 5.70, "R2": 0.86},
    ]

    best_model = "RandomForest"

    # Crear dos gráficas dummy
    comp_plot = _crear_grafica_dummy("model_comparison.png", "Comparación de modelos (dummy)")
    best_plot = _crear_grafica_dummy("best_model_scatter.png", "Modelo ganador (dummy)")

    return {
        "metrics": metrics,
        "best_model": best_model,
        "plots": [comp_plot, best_plot],
    }
