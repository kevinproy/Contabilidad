from __future__ import annotations

from typing import Any, Dict, List, Optional
from datetime import datetime
import traceback

from flask import Blueprint, render_template, request, redirect, url_for, flash, session

from services.db import db_connect, db_get_or_create_cliente, db_insert_movimiento
from services.estado_service import cargar_registros_estado, cargar_saldos_iniciales, cargar_anuladas, build_estado_df


bp = Blueprint("estado", __name__, url_prefix="/estado-cuenta")


def allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in {"xlsx"}


@bp.get("")
def submenu_estado_cuenta():
    resumen = session.pop("resumen_ec", None)
    return render_template("estado_cuenta.html", resumen=resumen)


@bp.post("/cargar")
def cargar_estado_cuenta():
    from services.estado_service import procesar_excel_stream
    archivo = request.files.get("excel")
    if not archivo or archivo.filename == "":
        msg = "Debe seleccionar un archivo .xlsx."
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return {"procesados": 0, "agregados": 0, "duplicados": 0, "no_agregados": 0, "errores": [msg]}, 400
        flash(msg, "error")
        return redirect(url_for("estado.submenu_estado_cuenta"))
    if not allowed_file(archivo.filename):
        msg = "Formato inválido. Solo se permite .xlsx."
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return {"procesados": 0, "agregados": 0, "duplicados": 0, "no_agregados": 0, "errores": [msg]}, 400
        flash(msg, "error")
        return redirect(url_for("estado.submenu_estado_cuenta"))

    try:
        nuevos_registros = procesar_excel_stream(archivo)
        procesados = len(nuevos_registros)
        agregados = 0
        duplicados = 0
        for r in nuevos_registros:
            client_name = str(r.get("CLIENTE", "")).strip()
            if not client_name:
                continue
            cid = db_get_or_create_cliente(client_name)
            if not cid:
                continue
            debe = float(r.get("DEBE", 0) or 0)
            haber = float(r.get("HABER", 0) or 0)
            tipo = "CARGO" if (debe > 0 and haber == 0) else ("PAGO" if (haber > 0 and debe == 0) else ("PAGO" if (haber - debe) >= 0 else "CARGO"))
            monto = round(abs(haber - debe), 2) if (debe > 0 and haber > 0) else (round(debe, 2) if tipo == "CARGO" else round(haber, 2))
            inserted = db_insert_movimiento(
                id_cliente=cid,
                fecha=str(r.get("FECHA", "")).strip() or None,
                tipo=tipo,
                monto=monto,
                descripcion=str(r.get("DETALLE", "")).strip() or None,
                docto=str(r.get("DOCTO", "")).strip() or None,
                int_ag=str(r.get("INT. AG.", "")).strip() or None,
                dim=str(r.get("DIM", "")).strip() or None,
                condicion_de_pago=str(r.get("CONDICION DE PAGO", "")).strip() or None,
            )
            if inserted:
                agregados += 1
            else:
                duplicados += 1
        no_agregados = max(0, procesados - agregados - duplicados)
        # total de registros en DB (vigentes)
        total_registros = 0
        try:
            with db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) AS total FROM movimientos WHERE anulada_en IS NULL")
                    rowc = cur.fetchone()
                    total_registros = int(rowc.get("total", 0) if isinstance(rowc, dict) else rowc[0])
        except Exception:
            total_registros = 0
        ahora = datetime.now()
        resumen = {
            'procesados': procesados,
            'agregados': agregados,
            'duplicados': duplicados,
            'no_agregados': no_agregados,
            'errores': [],
            'fecha': ahora.strftime('%d/%m/%Y'),
            'hora': ahora.strftime('%H:%M:%S'),
            'total_registros': total_registros,
        }
        session['resumen_ec'] = resumen
        flash(f"Procesados: {procesados} | Agregados: {agregados} | Duplicados: {duplicados}", "success")
        # Si es una XHR, retornar JSON directo
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return resumen, 200
    except Exception as exc:
        ahora = datetime.now()
        tb = traceback.format_exc()
        # Log al servidor para diagnóstico
        try:
            print("[UPLOAD_ERROR]", str(exc))
            print(tb)
        except Exception:
            pass
        resumen_err = {
            'procesados': 0,
            'agregados': 0,
            'duplicados': 0,
            'no_agregados': 0,
            'errores': [str(exc) or repr(exc), tb],
            'fecha': ahora.strftime('%d/%m/%Y'),
            'hora': ahora.strftime('%H:%M:%S'),
        }
        session['resumen_ec'] = resumen_err
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return resumen_err, 400
        flash(str(exc), "error")
    return redirect(url_for("estado.submenu_estado_cuenta"))


@bp.get("/tabla")
def ver_tabla_estado_cuenta():
    registros = cargar_registros_estado()
    cliente_q = request.args.get("cliente", "").strip()
    start_q = request.args.get("inicio", "").strip()
    end_q = request.args.get("fin", "").strip()
    sort_q = request.args.get("orden", "asc").strip().lower()

    saldos_ini = cargar_saldos_iniciales()
    df, total_debe, total_haber, total_saldo, clientes_opciones = build_estado_df(
        registros, saldos_ini, cliente_q, start_q, end_q, sort_q
    )

    columnas = ["FECHA", "DOCTO", "DETALLE", "INT. AG.", "DIM", "DEBE", "HABER", "SALDO", "VENCIMIENTO", "DIAS"]
    headers = ["FECHA", "DOCTO.", "DETALLE", "INT. AG.", "DIM", "DEBE", "HABER", "SALDO", "VENC.", "DIAS"]

    grupos: List[Dict[str, Any]] = []
    if not df.empty and cliente_q:
        sub = df[df["CLIENTE"].fillna("").astype(str).str.strip() == cliente_q].copy()
        extras = [c for c in ["MARK_DEBE", "MARK_HABER", "MARK_SALDO"] if c in sub.columns]
        # incluir columna auxiliar VENCIDO para estilos en plantilla
        aux_cols = ["VENCIDO"] if "VENCIDO" in sub.columns else []
        columnas_con_id = (["ID"] + columnas) if "ID" in sub.columns else columnas
        if extras or aux_cols:
            for cmark in extras:
                sub[cmark] = sub[cmark].fillna(0).astype(int)
            sub_view = sub[extras + aux_cols + columnas_con_id]
        else:
            sub_view = sub[columnas_con_id]
        grupos.append(
            {
                "cliente": cliente_q or "(Sin cliente)",
                "registros": sub_view.to_dict(orient="records"),
                "total_debe": round(float(sub["DEBE"].sum()), 2),
                "total_haber": round(float(sub["HABER"].sum()), 2),
                "total_saldo": round(float(sub.tail(1)["SALDO"].iloc[0]), 2) if not sub.empty else 0.0,
            }
        )

    return render_template(
        "tabla_estado_cuenta.html",
        columnas=columnas,
        headers=headers,
        registros=[],
        grupos=grupos,
        total_debe=total_debe,
        total_haber=total_haber,
        total_saldo=total_saldo,
        clientes=clientes_opciones,
        cliente_val=cliente_q,
        inicio_val=start_q,
        fin_val=end_q,
        orden_val=sort_q,
    )


@bp.get("/anuladas")
def ver_anuladas():
    registros = cargar_anuladas()
    if registros:
        import pandas as pd
        df = pd.DataFrame(registros)
        if "FECHA" in df.columns:
            df["_FECHA_DT"] = pd.to_datetime(df["FECHA"], errors="coerce")
            df["FECHA"] = df["_FECHA_DT"].dt.strftime("%d/%m/%Y").fillna(df["FECHA"])
        columnas_vista = ["FECHA","DOCTO","DETALLE","INT. AG.","DIM","DEBE","HABER","SALDO","ANULADO_EN"]
        for c in columnas_vista:
            if c not in df.columns:
                df[c] = ""
        registros_ordenados = df[columnas_vista].to_dict(orient="records")
    else:
        registros_ordenados = []
    return render_template(
        "tabla_anuladas.html",
        columnas=["FECHA","DOCTO.","DETALLE","INT. AG.","DIM","DEBE","HABER","SALDO","ANULADO EN"],
        registros=registros_ordenados,
    )


