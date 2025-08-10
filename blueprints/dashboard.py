
from flask import Blueprint, render_template
from services.db import db_connect
import pandas as pd
from datetime import datetime

bp = Blueprint("dashboard", __name__, url_prefix="/dashboard")

@bp.route("/")
def index():
    try:
        with db_connect() as conn:
            # Usar pandas para leer la consulta es eficiente
            sql = """
            SELECT
                c.nombre_completo AS cliente,
                m.docto,
                m.condicion_de_pago AS fecha_vencimiento,
                m.monto,
                m.tipo_de_movimiento
            FROM movimientos m
            JOIN clientes c ON c.id_cliente = m.id_cliente
            WHERE m.anulada_en IS NULL AND m.tipo_de_movimiento = 'CARGO' AND m.condicion_de_pago IS NOT NULL
            """
            df = pd.read_sql_query(sql, conn)
    except Exception as e:
        # Si hay un error de DB (como el de psycopg), mostrar dashboard vacío
        print(f"ERROR LEYENDO DATOS PARA DASHBOARD: {e}")
        df = pd.DataFrame()

    kpis = {
        "total_mora": 0,
        "facturas_vencidas": 0,
        "top_clientes_mora": [],
        "aging_data": {"labels": [], "values": []}
    }
    documentos_vencidos = []

    if not df.empty:
        df["fecha_vencimiento"] = pd.to_datetime(df["fecha_vencimiento"], errors="coerce")
        df = df.dropna(subset=["fecha_vencimiento"]) # Ignorar filas sin fecha de vencimiento válida

        hoy = datetime.now()
        df_mora = df[df["fecha_vencimiento"] < hoy].copy()

        if not df_mora.empty:
            df_mora["dias_mora"] = (hoy - df_mora["fecha_vencimiento"]).dt.days
            df_mora["monto"] = pd.to_numeric(df_mora["monto"], errors="coerce").fillna(0)

            # KPI: Total en Mora
            kpis["total_mora"] = round(df_mora["monto"].sum(), 2)
            kpis["facturas_vencidas"] = len(df_mora)

            # Tabla de documentos vencidos
            documentos_vencidos = df_mora.sort_values(by="dias_mora", ascending=False).to_dict("records")

            # Gráfico: Top 5 Clientes en Mora
            top_clientes = df_mora.groupby("cliente")["monto"].sum().nlargest(5)
            kpis["top_clientes_mora"] = [{"cliente": index, "monto": round(value, 2)} for index, value in top_clientes.items()]

            # Gráfico: Antigüedad de Deuda (Aging Report)
            bins = [0, 30, 60, 90, float("inf")]
            labels = ["1-30 días", "31-60 días", "61-90 días", "90+ días"]
            df_mora["aging_group"] = pd.cut(df_mora["dias_mora"], bins=bins, labels=labels, right=True)
            aging_summary = df_mora.groupby("aging_group")["monto"].sum()
            kpis["aging_data"] = {
                "labels": aging_summary.index.tolist(),
                "values": [round(v, 2) for v in aging_summary.values]
            }

    return render_template("dashboard.html", kpis=kpis, documentos=documentos_vencidos)
