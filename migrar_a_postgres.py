"""
Script de migración de datos desde JSON a PostgreSQL.

Lee `estado_de_cuenta.json` (lista de transacciones) y `saldos_iniciales.json`
(mapa por cliente) y los inserta en tablas `clientes` y `movimientos`.

Ejecución (Windows PowerShell):
  $env:PGHOST="localhost"; $env:PGPORT="5432"; $env:PGDATABASE="mi_db"; \
  $env:PGUSER="mi_usuario"; $env:PGPASSWORD="mi_password"; \
  python migrar_a_postgres.py

Requiere: psycopg2-binary
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import execute_values


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ESTADO_JSON = os.path.join(BASE_DIR, "estado_de_cuenta.json")
SALDOS_JSON = os.path.join(BASE_DIR, "saldos_iniciales.json")


@dataclass
class DbConfig:
    host: str
    port: int
    database: str
    user: str
    password: str


def get_db_config_from_env() -> DbConfig:
    missing: List[str] = []
    host = os.environ.get("PGHOST") or "localhost"
    port_txt = os.environ.get("PGPORT") or "5432"
    db = os.environ.get("PGDATABASE")
    user = os.environ.get("PGUSER")
    pwd = os.environ.get("PGPASSWORD")
    for key, val in (("PGDATABASE", db), ("PGUSER", user), ("PGPASSWORD", pwd)):
        if not val:
            missing.append(key)
    if missing:
        raise SystemExit(
            "Faltan variables de entorno: " + ", ".join(missing) +
            " | Ejemplo: set PGHOST=localhost; set PGPORT=5432; set PGDATABASE=mi_db; set PGUSER=mi_user; set PGPASSWORD=mi_pwd"
        )
    try:
        port = int(port_txt)
    except Exception:
        port = 5432
    return DbConfig(host=host, port=port, database=db, user=user, password=pwd)


def connect_db(cfg: DbConfig):
    return psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.database,
        user=cfg.user,
        password=cfg.password,
    )


DDL_CLIENTES = """
CREATE TABLE IF NOT EXISTS clientes (
    id_cliente SERIAL PRIMARY KEY,
    nombre_completo VARCHAR(255) UNIQUE NOT NULL,
    correo_electronico VARCHAR(255),
    telefono VARCHAR(50)
);
"""

DDL_MOVIMIENTOS = """
CREATE TABLE IF NOT EXISTS movimientos (
    id_movimiento SERIAL PRIMARY KEY,
    id_cliente INTEGER NOT NULL REFERENCES clientes(id_cliente) ON DELETE CASCADE,
    fecha DATE,
    tipo_de_movimiento VARCHAR(50) NOT NULL,
    monto NUMERIC(10, 2) NOT NULL,
    descripcion TEXT
);
"""


def ensure_schema(cur) -> None:
    cur.execute(DDL_CLIENTES)
    cur.execute(DDL_MOVIMIENTOS)
    # Índice único natural para evitar duplicados al re-migrar
    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_movimientos_natural
        ON public.movimientos (id_cliente, fecha, tipo_de_movimiento, monto, descripcion)
        """
    )


def read_json_files() -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    # estado_de_cuenta.json
    if not os.path.exists(ESTADO_JSON):
        print(f"[WARN] No existe {ESTADO_JSON}. Continuando con lista vacía.")
        estado: List[Dict[str, Any]] = []
    else:
        with open(ESTADO_JSON, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
                estado = data if isinstance(data, list) else []
            except json.JSONDecodeError:
                estado = []

    # saldos_iniciales.json
    saldos: Dict[str, Dict[str, Any]]
    if not os.path.exists(SALDOS_JSON):
        print(f"[INFO] No existe {SALDOS_JSON}. Sin saldos iniciales.")
        saldos = {}
    else:
        with open(SALDOS_JSON, "r", encoding="utf-8") as f:
            try:
                raw = json.load(f)
                saldos = raw if isinstance(raw, dict) else {}
            except json.JSONDecodeError:
                saldos = {}

    return estado, saldos


def normalize_string(value: Any) -> str:
    if value is None:
        return ""
    s = str(value).strip()
    return s


def normalize_number(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        try:
            return float(value)
        except Exception:
            return 0.0
    s = str(value).strip()
    if s == "":
        return 0.0
    try:
        return float(s)
    except Exception:
        s2 = s.replace(".", "").replace(",", ".")
        try:
            return float(s2)
        except Exception:
            return 0.0


def upsert_clientes(cur, nombres: List[str]) -> Dict[str, int]:
    # Insertar ignorando duplicados por UNIQUE(nombre_completo)
    if not nombres:
        return {}
    uniq = sorted({normalize_string(n) for n in nombres if normalize_string(n)})
    # Inserción masiva con ON CONFLICT DO NOTHING
    values = [(n,) for n in uniq]
    execute_values(
        cur,
        "INSERT INTO clientes (nombre_completo) VALUES %s ON CONFLICT (nombre_completo) DO NOTHING",
        values,
    )
    # Obtener mapping nombre -> id_cliente
    cur.execute(
        "SELECT id_cliente, nombre_completo FROM clientes WHERE nombre_completo = ANY(%s)",
        (uniq,),
    )
    rows = cur.fetchall()
    return {r[1]: r[0] for r in rows}


def map_tipo_movimiento(debe: float, haber: float) -> Tuple[str, float]:
    debe = float(debe or 0)
    haber = float(haber or 0)
    if debe > 0 and haber == 0:
        return "CARGO", round(debe, 2)
    if haber > 0 and debe == 0:
        return "PAGO", round(haber, 2)
    # Caso mixto o cero: priorizamos signo neto
    neto = haber - debe
    if neto >= 0:
        return "PAGO", round(abs(neto), 2)
    return "CARGO", round(abs(neto), 2)


def build_movimientos_estado(estado: List[Dict[str, Any]]) -> List[Tuple[int, Optional[str], str, float, Optional[str]]]:
    """Convierte registros del JSON a filas para INSERT en `movimientos`.

    Retorna una lista de tuplas con:
      (id_cliente, fecha, tipo_de_movimiento, monto, descripcion)
    """
    movimientos: List[Tuple[int, Optional[str], str, float, Optional[str]]] = []
    return movimientos


def main() -> None:
    cfg = get_db_config_from_env()
    estado, saldos = read_json_files()

    # Preparar catálogo de clientes a partir de ambos orígenes
    clientes_estado = [normalize_string(r.get("CLIENTE")) for r in estado]
    clientes_saldos = list(saldos.keys())
    all_clientes = clientes_estado + clientes_saldos

    with connect_db(cfg) as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            ensure_schema(cur)

            nombre_to_id = upsert_clientes(cur, all_clientes)

            # Insertar saldos iniciales como movimientos
            saldo_rows: List[Tuple[int, Optional[str], str, float, Optional[str]]] = []
            for nombre, info in saldos.items():
                nombre_norm = normalize_string(nombre)
                if not nombre_norm:
                    continue
                id_cliente = nombre_to_id.get(nombre_norm)
                if not id_cliente:
                    continue
                monto = normalize_number(info.get("monto"))
                lado = normalize_string(info.get("lado")).lower()
                fecha = normalize_string(info.get("fecha")) or None
                # lado 'debe' => CARGO, 'haber' => PAGO
                tipo = "CARGO" if lado == "debe" else "PAGO"
                saldo_rows.append((id_cliente, fecha, tipo, round(abs(monto), 2), "SALDO ANTERIOR"))

            if saldo_rows:
                execute_values(
                    cur,
                    (
                        "INSERT INTO movimientos (id_cliente, fecha, tipo_de_movimiento, monto, descripcion) "
                        "VALUES %s ON CONFLICT (id_cliente, fecha, tipo_de_movimiento, monto, descripcion) DO NOTHING"
                    ),
                    saldo_rows,
                )

            # Insertar movimientos del estado de cuenta
            mov_rows: List[Tuple[int, Optional[str], str, float, Optional[str]]] = []
            for r in estado:
                nombre = normalize_string(r.get("CLIENTE"))
                if not nombre:
                    continue
                id_cliente = nombre_to_id.get(nombre)
                if not id_cliente:
                    continue
                fecha = normalize_string(r.get("FECHA")) or None
                detalle = normalize_string(r.get("DETALLE")) or None
                debe = normalize_number(r.get("DEBE"))
                haber = normalize_number(r.get("HABER"))
                tipo, monto = map_tipo_movimiento(debe, haber)
                mov_rows.append((id_cliente, fecha, tipo, monto, detalle))

            if mov_rows:
                execute_values(
                    cur,
                    (
                        "INSERT INTO movimientos (id_cliente, fecha, tipo_de_movimiento, monto, descripcion) "
                        "VALUES %s ON CONFLICT (id_cliente, fecha, tipo_de_movimiento, monto, descripcion) DO NOTHING"
                    ),
                    mov_rows,
                )

        conn.commit()
    print("[OK] Migración completada.")


if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        raise
    except Exception as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)


