from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta

import pandas as pd

from services.db import db_connect, db_get_or_create_cliente, db_insert_movimiento


def cargar_registros_estado() -> List[Dict[str, Any]]:
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT m.id_movimiento AS "ID",
                       c.nombre_completo AS "CLIENTE",
                       COALESCE(TO_CHAR(m.fecha, 'YYYY-MM-DD'), '') AS "FECHA",
                       m.docto AS "DOCTO",
                       m.descripcion AS "DETALLE",
                       m.int_ag AS "INT. AG.",
                       m.dim AS "DIM",
                       COALESCE(TO_CHAR(m.condicion_de_pago, 'YYYY-MM-DD'), '') AS "CONDICION DE PAGO",
                       m.tipo_de_movimiento,
                       m.monto,
                       m.order_index AS "ORDER",
                       COALESCE(TO_CHAR(m.pagado_en,'YYYY-MM-DD'),'') AS "PAGADO_EN",
                       COALESCE(m.mark_debe,0) AS MARK_DEBE,
                       COALESCE(m.mark_haber,0) AS MARK_HABER,
                       COALESCE(m.mark_saldo,0) AS MARK_SALDO
                FROM movimientos m
                JOIN clientes c ON c.id_cliente = m.id_cliente
                WHERE m.anulada_en IS NULL
                """
            )
            rows = cur.fetchall()
            out: List[Dict[str, Any]] = []
            for r in rows:
                debe = float(r["monto"]) if r["tipo_de_movimiento"] == "CARGO" else 0.0
                haber = float(r["monto"]) if r["tipo_de_movimiento"] == "PAGO" else 0.0
                out.append({
                    "ID": str(r["ID"]),
                    "CLIENTE": r["CLIENTE"],
                    "FECHA": r["FECHA"],
                    "DOCTO": r["DOCTO"] or "",
                    "DETALLE": r["DETALLE"] or "",
                    "INT. AG.": r["INT. AG."] or "",
                    "DIM": r["DIM"] or "",
                    "CONDICION DE PAGO": r["CONDICION DE PAGO"] or "",
                    "DEBE": round(debe, 2),
                    "HABER": round(haber, 2),
                    "SALDO": 0.0,
                    "ORDER": r.get("ORDER"),
                    "PAGADO_EN": r.get("PAGADO_EN") or "",
                    "MARK_DEBE": int(r.get("MARK_DEBE", 0)),
                    "MARK_HABER": int(r.get("MARK_HABER", 0)),
                    "MARK_SALDO": int(r.get("MARK_SALDO", 0)),
                })
            return out


def cargar_anuladas() -> List[Dict[str, Any]]:
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT c.nombre_completo AS "CLIENTE",
                       COALESCE(TO_CHAR(m.fecha,'YYYY-MM-DD'),'') AS "FECHA",
                       m.docto AS "DOCTO",
                       m.descripcion AS "DETALLE",
                       m.int_ag AS "INT. AG.",
                       m.dim AS "DIM",
                       m.tipo_de_movimiento,
                       m.monto,
                       COALESCE(TO_CHAR(m.anulada_en,'YYYY-MM-DD HH24:MI:SS'),'') AS "ANULADO_EN"
                FROM movimientos m
                JOIN clientes c ON c.id_cliente = m.id_cliente
                WHERE m.anulada_en IS NOT NULL
                ORDER BY m.anulada_en DESC
                """
            )
            rows = cur.fetchall()
            out: List[Dict[str, Any]] = []
            for r in rows:
                debe = float(r["monto"]) if r["tipo_de_movimiento"] == "CARGO" else 0.0
                haber = float(r["monto"]) if r["tipo_de_movimiento"] == "PAGO" else 0.0
                out.append({
                    "FECHA": r["FECHA"],
                    "DOCTO": r["DOCTO"] or "",
                    "DETALLE": r["DETALLE"] or "",
                    "INT. AG.": r["INT. AG."] or "",
                    "DIM": r["DIM"] or "",
                    "DEBE": round(debe, 2),
                    "HABER": round(haber, 2),
                    "SALDO": 0.0,
                    "ANULADO_EN": r["ANULADO_EN"],
                })
            return out


def cargar_saldos_iniciales() -> Dict[str, Dict[str, Any]]:
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT c.nombre_completo AS cliente, s.monto, s.lado,
                       COALESCE(TO_CHAR(s.fecha,'YYYY-MM-DD'),'2025-01-01') AS fecha
                FROM saldos_iniciales s
                JOIN clientes c ON c.id_cliente = s.id_cliente
                """
            )
            rows = cur.fetchall()
            return {r["cliente"]: {"monto": float(r["monto"] or 0), "lado": r["lado"], "fecha": r["fecha"]} for r in rows}


def build_estado_df(registros: List[Dict[str, Any]], saldos_ini: Optional[Dict[str, Dict[str, Any]]] = None,
                    cliente_q: str = "", start_q: str = "", end_q: str = "", sort_q: str = "asc") -> Tuple[pd.DataFrame, float, float, float, List[str]]:
    df = pd.DataFrame(registros)
    if df.empty:
        return pd.DataFrame(), 0.0, 0.0, 0.0, []

    columnas_salida = [
        "FECHA", "DOCTO", "DETALLE", "DEBE", "HABER", "SALDO", "INT. AG.", "DIM", "CONDICION DE PAGO", "VENCIMIENTO", "DIAS", "VENCIDO",
    ]
    for col in columnas_salida + ["CLIENTE"]:
        if col not in df.columns:
            df[col] = "" if col not in ["DEBE", "HABER", "SALDO"] else 0.0

    df["DEBE"] = pd.to_numeric(df["DEBE"], errors="coerce").fillna(0.0)
    df["HABER"] = pd.to_numeric(df["HABER"], errors="coerce").fillna(0.0)
    df["SALDO"] = pd.to_numeric(df["SALDO"], errors="coerce").fillna(0.0)
    df["_FECHA_DT"] = pd.to_datetime(df["FECHA"], errors="coerce")
    df["_VENC_DT"] = pd.to_datetime(df["CONDICION DE PAGO"], errors="coerce")

    df["DETALLE"] = df.apply(
        lambda r: f"{str(r.get('CLIENTE') or '').strip()} - {str(r.get('DOCTO') or '').strip()}".strip(" -")
        if str(r.get("CLIENTE") or "").strip() or str(r.get("DOCTO") or "").strip()
        else r.get("DETALLE"),
        axis=1,
    )

    clientes_opciones = (
        df["CLIENTE"].dropna().astype(str).str.strip().replace({"nan": ""}).unique().tolist()
    )
    clientes_opciones = sorted([c for c in clientes_opciones if c])

    if cliente_q:
        df = df[df["CLIENTE"].astype(str).str.strip() == cliente_q]
    if start_q:
        start_dt = pd.to_datetime(start_q, errors="coerce")
        if pd.notna(start_dt):
            df = df[df["_FECHA_DT"] >= start_dt]
    if end_q:
        end_dt = pd.to_datetime(end_q, errors="coerce")
        if pd.notna(end_dt):
            df = df[df["_FECHA_DT"] <= end_dt]

    df_calc = df.copy()
    df_calc["_ORDER"] = pd.to_numeric(df_calc.get("ORDER"), errors="coerce")
    df_calc = df_calc.sort_values(by=["CLIENTE", "_ORDER", "_FECHA_DT", "DOCTO"], ascending=[True, True, True, True], na_position="last")
    df_calc["_MOV"] = df_calc["HABER"] - df_calc["DEBE"]
    df_calc["SALDO"] = df_calc.groupby("CLIENTE")["_MOV"].cumsum().round(2)
    df_calc["_INI"] = 1

    # Calcular vencimiento (solo FA/PG) = fecha + 30 días, y estado vencido
    def calc_venc(row):
        tipo = str(row.get("DOCTO") or "").strip().upper()
        fdt = row.get("_FECHA_DT")
        # días de mora personalizados: por ahora 30 (placeholder para integrar cliente_prefs si se pasa en registros)
        dias_mora = 30
        if pd.notna(fdt) and (tipo.startswith("FA") or tipo.startswith("PG")):
            return fdt + timedelta(days=dias_mora)
        return pd.NaT
    df_calc["_VENC_CALC"] = df_calc.apply(calc_venc, axis=1)
    today = pd.to_datetime(pd.Timestamp.today().date())
    # Si está pagado, no debe marcar como vencido ni mostrar días
    pagado_series = df.get("PAGADO_EN") if "PAGADO_EN" in df.columns else None
    pagado_dt = pd.to_datetime(pagado_series, errors="coerce") if pagado_series is not None else None
    df_calc["VENCIMIENTO"] = df_calc["_VENC_CALC"].dt.strftime("%d/%m/%Y").fillna("")
    dias_calc = (today - df_calc["_VENC_CALC"]).dt.days
    if pagado_dt is not None:
        # si pagado, días = 0 y vencido = 0
        dias_calc = dias_calc.where(pagado_dt.isna(), 0)
    df_calc["DIAS"] = dias_calc.fillna(0).astype(int)
    vencido_calc = ((today > df_calc["_VENC_CALC"]) & df_calc["_VENC_CALC"].notna()).astype(int)
    if pagado_dt is not None:
        vencido_calc = vencido_calc.where(pagado_dt.isna(), 0)
    df_calc["VENCIDO"] = vencido_calc

    if saldos_ini:
        def agregar_ini(grupo: pd.DataFrame) -> pd.DataFrame:
            cliente = str(grupo["CLIENTE"].iloc[0])
            info = saldos_ini.get(cliente)
            if not info:
                return grupo
            monto = float(info.get("monto", 0) or 0)
            lado = str(info.get("lado", "haber")).lower()
            mov = monto if lado == "haber" else -monto
            fecha_txt = info.get("fecha", "2025-01-01")
            try:
                fecha_dt = pd.to_datetime(fecha_txt)
            except Exception:
                fecha_dt = pd.to_datetime("2025-01-01")
            fila_ini = {
                "CLIENTE": cliente,
                "FECHA": fecha_dt.strftime("%d/%m/%Y"),
                "_FECHA_DT": fecha_dt,
                "DOCTO": "SALDO ANTERIOR",
                "DETALLE": f"{cliente} - SALDO ANTERIOR",
                "INT. AG.": "",
                "DIM": "",
                "DEBE": monto if lado == "debe" else 0.0,
                "HABER": monto if lado == "haber" else 0.0,
                "_MOV": mov,
                "SALDO": 0.0,
                "_INI": 0,
            }
            nuevo = pd.concat([pd.DataFrame([fila_ini]), grupo], ignore_index=True)
            nuevo["SALDO"] = nuevo["_MOV"].cumsum().round(2)
            return nuevo
        df_calc = df_calc.groupby("CLIENTE", as_index=False, group_keys=False).apply(agregar_ini)

    df = df_calc.reset_index(drop=True)
    total_debe = round(float(df["DEBE"].sum()), 2)
    total_haber = round(float(df["HABER"].sum()), 2)
    ultimos = df_calc.dropna(subset=["CLIENTE"]).groupby("CLIENTE").tail(1)
    total_saldo = round(float(ultimos["SALDO"].sum()), 2)

    asc = (sort_q or "asc").lower() != "desc"
    if "_INI" not in df.columns:
        df["_INI"] = 1
    # Respetar orden manual persistido (ORDER) primero, luego fecha/docto
    if "_ORDER" not in df.columns:
        df["_ORDER"] = pd.to_numeric(df.get("ORDER"), errors="coerce")
    df = df.sort_values(by=["_INI", "_ORDER", "_FECHA_DT", "DOCTO"], ascending=[True, True, asc, asc], na_position="last")
    # Copiar columnas calculadas
    for extra in ["VENCIMIENTO", "DIAS", "VENCIDO"]:
        df[extra] = df_calc[extra].values

    df["FECHA"] = df["_FECHA_DT"].dt.strftime("%d/%m/%Y").fillna("")
    df["CONDICION DE PAGO"] = df["_VENC_DT"].dt.strftime("%d/%m/%Y").fillna(df["CONDICION DE PAGO"].fillna(""))

    return df, total_debe, total_haber, total_saldo, clientes_opciones


# ===== Helpers de Excel (validación y normalización) =====

# Columnas mínimas requeridas (flexibles)
MIN_COLS = ["CLIENTE", "FECHA"]


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Normaliza encabezados (upper, colapsa espacios, conserva puntos)
    def norm(name: str) -> str:
        s = str(name).strip().upper()
        s = " ".join(s.split())
        return s

    df = df.rename(columns={c: norm(c) for c in df.columns})

    # Aliases comunes → nombres esperados
    aliases = {
        "DOC": "DOC.", "DOCTO": "DOC.", "DOCUMENTO": "DOC.",
        "NRO FAC": "NRO. FAC.", "NRO. FAC": "NRO. FAC.",
        "NRO DOC": "NRO. DOC.", "NRO. DOC": "NRO. DOC.",
        "DEBE": "DEBITO", "HABER": "CREDITO",
        "SALDO FINAL": "SALDO",
        "DUI": "DUI/DUE", "DUI DUE": "DUI/DUE",
        "REFERENCIA": "REFER.", "REFER": "REFER.",
    }
    for k, v in list(aliases.items()):
        if k in df.columns and v not in df.columns:
            df = df.rename(columns={k: v})
    return df


def validar_columnas_excel(df: pd.DataFrame) -> List[str]:
    # Requiere CLIENTE, FECHA y al menos uno de DEBITO/CREDITO (los demás, opcionales)
    faltantes: List[str] = []
    for c in MIN_COLS:
        if c not in df.columns:
            faltantes.append(c)
    if "DEBITO" not in df.columns and "CREDITO" not in df.columns:
        faltantes.append("DEBITO/CREDITO")
    return faltantes


def normalizar_fecha(valor: Any) -> str:
    if pd.isna(valor):
        return ""
    if isinstance(valor, datetime):
        return valor.strftime("%Y-%m-%d")
    if hasattr(valor, "to_pydatetime"):
        try:
            return valor.to_pydatetime().strftime("%Y-%m-%d")
        except Exception:
            pass
    cadena = str(valor).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(cadena, fmt).strftime("%Y-%m-%d")
        except Exception:
            continue
    return cadena


def a_cadena_segura(valor: Any) -> str:
    if pd.isna(valor):
        return ""
    return str(valor).strip()


def a_numero_seguro(valor: Any) -> float:
    if pd.isna(valor):
        return 0.0
    if isinstance(valor, (int, float)):
        try:
            return float(valor)
        except Exception:
            return 0.0
    texto = str(valor).strip()
    if texto == "":
        return 0.0
    texto_norm = texto
    if texto.count(",") == 1 and texto.count(".") > 1:
        texto_norm = texto.replace(".", "").replace(",", ".")
    try:
        return float(texto_norm)
    except Exception:
        limpio = "".join(ch for ch in texto_norm if ch.isdigit() or ch in "-.")
        try:
            return float(limpio)
        except Exception:
            return 0.0


def procesar_excel_stream(file_stream) -> List[Dict[str, Any]]:
    try:
        df = pd.read_excel(file_stream, engine="openpyxl")
    except Exception as exc:
        raise ValueError(f"No fue posible leer el Excel: {exc}") from exc

    # Normalizar encabezados y validar
    df = _normalize_columns(df)
    faltantes = validar_columnas_excel(df)
    if faltantes:
        raise ValueError(
            "El archivo Excel no contiene todas las columnas requeridas. "
            f"Faltantes: {', '.join(faltantes)}"
        )

    registros_salida: List[Dict[str, Any]] = []
    for _, fila in df.iterrows():
        cliente = a_cadena_segura(fila.get("CLIENTE"))
        fecha = normalizar_fecha(fila.get("FECHA"))
        doc = a_cadena_segura(fila.get("DOC."))
        nro_fac = a_cadena_segura(fila.get("NRO. FAC."))
        concepto = a_cadena_segura(fila.get("CONCEPTO"))
        mercaderia = a_cadena_segura(fila.get("MERCADERIA"))
        # Aceptar DEBITO/CREDITO o DEBE/HABER (normalizados arriba)
        debito = a_numero_seguro(fila.get("DEBITO"))
        credito = a_numero_seguro(fila.get("CREDITO"))
        saldo = a_numero_seguro(fila.get("SALDO"))
        tramite = a_cadena_segura(fila.get("TRAMITE"))
        dui_due = a_cadena_segura(fila.get("DUI/DUE"))

        docto = (doc + " " + nro_fac).strip()
        detalle_fallback = (concepto + (" - " + mercaderia if mercaderia else "")).strip()
        detalle = (cliente + " - " + docto).strip(" -") if cliente or docto else detalle_fallback

        condicion_pago = ""
        tipo_docto = (doc or "").strip().upper()
        if tipo_docto in {"FA", "PG"} and fecha:
            try:
                due = datetime.strptime(fecha, "%Y-%m-%d") + timedelta(days=30)
                condicion_pago = due.strftime("%Y-%m-%d")
            except Exception:
                condicion_pago = ""

        registros_salida.append({
            "CLIENTE": cliente,
            "FECHA": fecha,
            "DOCTO": docto,
            "DETALLE": detalle,
            "DEBE": round(debito, 2),
            "HABER": round(credito, 2),
            "SALDO": round(saldo, 2),
            "INT. AG.": tramite,
            "DIM": dui_due,
            "CONDICION DE PAGO": condicion_pago,
        })

    return registros_salida


def upsert_saldo_inicial(cliente: str, monto: float, lado: str, fecha: str = "2025-01-01") -> None:
    cid = db_get_or_create_cliente(cliente)
    if not cid:
        return
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO saldos_iniciales (id_cliente, monto, lado, fecha)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT (id_cliente) DO UPDATE SET monto=EXCLUDED.monto, lado=EXCLUDED.lado, fecha=EXCLUDED.fecha
                """,
                (cid, float(monto or 0), lado, fecha),
            )
        conn.commit()


def delete_saldo_inicial(cliente: str) -> None:
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM saldos_iniciales USING clientes c WHERE saldos_iniciales.id_cliente=c.id_cliente AND c.nombre_completo=%s", (cliente,))
        conn.commit()



