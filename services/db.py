from __future__ import annotations

import os
from typing import Any, Dict, Optional

import psycopg
from psycopg.rows import dict_row


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.abspath(os.path.join(BASE_DIR, os.pardir))


# En Render, las variables de entorno ya están disponibles y no se necesita .env
# La lógica de mapeo DB_* a PG* y SSL se manejará directamente en db_connect.
# La función load_env ya no es necesaria.


def db_connect():
    cfg: Dict[str, Any] = {
        "host": os.environ.get("DB_HOST"),
        "port": int(os.environ.get("DB_PORT", "5432")),
        "dbname": os.environ.get("DB_NAME"),
        "user": os.environ.get("DB_USER"),
        "password": os.environ.get("DB_PASSWORD"),
    }
    # For Supabase, require SSL by default if not specified
    host = os.environ.get("DB_HOST", "")
    if ("supabase.com" in host) and not os.environ.get("PGSSLMODE"):
        cfg["sslmode"] = "require"
    
    return psycopg.connect(**cfg, row_factory=dict_row, prepare_threshold=None)


def ensure_db_schema() -> None:
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS clientes (
                    id_cliente SERIAL PRIMARY KEY,
                    nombre_completo VARCHAR(255) UNIQUE NOT NULL,
                    correo_electronico VARCHAR(255),
                    telefono VARCHAR(50)
                );
                """
            )
            # Usuarios y seguridad (RBAC simple)
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id_user SERIAL PRIMARY KEY,
                    username VARCHAR(120) UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    is_master BOOLEAN NOT NULL DEFAULT FALSE,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS permissions (
                    code VARCHAR(120) PRIMARY KEY,
                    description VARCHAR(255)
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS user_permissions (
                    id_user INTEGER NOT NULL REFERENCES users(id_user) ON DELETE CASCADE,
                    perm_code VARCHAR(120) NOT NULL REFERENCES permissions(code) ON DELETE CASCADE,
                    PRIMARY KEY (id_user, perm_code)
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS movimientos (
                    id_movimiento SERIAL PRIMARY KEY,
                    id_cliente INTEGER NOT NULL REFERENCES clientes(id_cliente) ON DELETE CASCADE,
                    fecha DATE,
                    tipo_de_movimiento VARCHAR(50) NOT NULL,
                    monto NUMERIC(10,2) NOT NULL,
                    descripcion TEXT,
                    docto VARCHAR(255),
                    int_ag VARCHAR(255),
                    dim VARCHAR(255),
                    condicion_de_pago DATE,
                    order_index INTEGER,
                    mark_debe INTEGER DEFAULT 0,
                    mark_haber INTEGER DEFAULT 0,
                    mark_saldo INTEGER DEFAULT 0,
                    anulada_en TIMESTAMP WITHOUT TIME ZONE
                );
                CREATE UNIQUE INDEX IF NOT EXISTS ux_movimientos_natural
                ON movimientos (id_cliente, fecha, tipo_de_movimiento, monto, descripcion);
                """
            )
            # Campos adicionales opcionales
            try:
                cur.execute("ALTER TABLE movimientos ADD COLUMN IF NOT EXISTS pagado_en DATE")
            except Exception:
                pass
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS saldos_iniciales (
                    id_cliente INTEGER PRIMARY KEY REFERENCES clientes(id_cliente) ON DELETE CASCADE,
                    monto NUMERIC(12,2) NOT NULL DEFAULT 0,
                    lado VARCHAR(10) NOT NULL DEFAULT 'haber',
                    fecha DATE NOT NULL DEFAULT DATE '2025-01-01'
                );
                """
            )
            # Preferencias por cliente (días de mora personalizados)
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS cliente_prefs (
                    id_cliente INTEGER PRIMARY KEY REFERENCES clientes(id_cliente) ON DELETE CASCADE,
                    dias_mora INTEGER NOT NULL DEFAULT 30
                );
                """
            )
            # Empleados y planilla
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS empleados (
                    id_empleado SERIAL PRIMARY KEY,
                    carnet VARCHAR(50) UNIQUE,
                    nombres_apellidos VARCHAR(255) NOT NULL,
                    cargo VARCHAR(120),
                    cua VARCHAR(50),
                    cns VARCHAR(50),
                    cns_patronal VARCHAR(50),
                    fecha_ingreso DATE,
                    haber_basico NUMERIC(12,2) NOT NULL DEFAULT 0
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS planilla_items (
                    id_item SERIAL PRIMARY KEY,
                    periodo_yyyymm VARCHAR(6) NOT NULL,
                    id_empleado INTEGER NOT NULL REFERENCES empleados(id_empleado) ON DELETE CASCADE,
                    dias_trab INTEGER DEFAULT 30,
                    bono_antiguedad NUMERIC(12,2) DEFAULT 0,
                    otros_ingresos NUMERIC(12,2) DEFAULT 0,
                    total_ganado NUMERIC(12,2) DEFAULT 0,
                    afp_1271 NUMERIC(12,2) DEFAULT 0,
                    ap_solidario NUMERIC(12,2) DEFAULT 0,
                    quincena NUMERIC(12,2) DEFAULT 0,
                    anticipos NUMERIC(12,2) DEFAULT 0,
                    prestamos NUMERIC(12,2) DEFAULT 0,
                    entel NUMERIC(12,2) DEFAULT 0,
                    otros_desc NUMERIC(12,2) DEFAULT 0,
                    atrasos NUMERIC(12,2) DEFAULT 0,
                    rc_iva NUMERIC(12,2) DEFAULT 0,
                    total_afps NUMERIC(12,2) DEFAULT 0,
                    total_anticipos NUMERIC(12,2) DEFAULT 0,
                    total_desc NUMERIC(12,2) DEFAULT 0,
                    total_afp_ant_desc NUMERIC(12,2) DEFAULT 0,
                    liquido_pagable NUMERIC(12,2) DEFAULT 0,
                    rc_iva_acum NUMERIC(12,2) DEFAULT 0,
                    UNIQUE (periodo_yyyymm, id_empleado)
                );
                """
            )
            # snapshots de planilla por periodo
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS planilla_snapshots (
                    periodo_yyyymm VARCHAR(6) NOT NULL,
                    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
                    data JSONB NOT NULL,
                    PRIMARY KEY (periodo_yyyymm, created_at)
                );
                """
            )
        conn.commit()


def seed_permissions_and_master() -> None:
    """Ensure base permission codes and a default master admin exist."""
    from werkzeug.security import generate_password_hash

    base_perms = [
        ("dashboard:view", "Ver Dashboard"),
        ("estado:view", "Ver Estado de Cuenta"),
        ("estado:upload", "Cargar/Editar Estado de Cuenta"),
        ("estado:export", "Exportar Estado de Cuenta"),
        ("estado:anuladas:view", "Ver Anuladas"),
        ("estado:saldos:manage", "Gestionar Saldos Anteriores"),
        ("estado:prefs:manage", "Gestionar Preferencias de Cliente"),
        ("planilla:view", "Ver Planilla de Sueldos"),
        ("planilla:edit", "Editar Planilla de Sueldos"),
        ("planilla:empleados:view", "Ver Registro de Trabajadores"),
        ("planilla:periodo:view", "Ver Planilla Mensual"),
        ("admin:users", "Administrar Usuarios y Permisos"),
    ]
    with db_connect() as conn:
        with conn.cursor() as cur:
            for code, desc in base_perms:
                cur.execute(
                    "INSERT INTO permissions (code, description) VALUES (%s,%s) ON CONFLICT (code) DO NOTHING",
                    (code, desc),
                )
            # Ensure at least one master user
            cur.execute("SELECT COUNT(*) AS n FROM users")
            row = cur.fetchone()
            total = int(row["n"] if isinstance(row, dict) else row[0])
            if total == 0:
                cur.execute(
                    """
                    INSERT INTO users (username, password_hash, is_master)
                    VALUES (%s,%s,TRUE)
                    RETURNING id_user
                    """,
                    ("admin", generate_password_hash("admin123")),
                )
                admin_id = (cur.fetchone()["id_user"])  # assign all perms to master optionally
                # Not strictly necessary since master bypasses, but grant perms too
                for code, _ in base_perms:
                    cur.execute(
                        "INSERT INTO user_permissions (id_user, perm_code) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                        (admin_id, code),
                    )
        conn.commit()


def db_get_or_create_cliente(nombre: str) -> Optional[int]:
    nombre_norm = (nombre or "").strip()
    if not nombre_norm:
        return None
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id_cliente FROM clientes WHERE nombre_completo=%s", (nombre_norm,))
            row = cur.fetchone()
            if row:
                return int(row["id_cliente"]) if isinstance(row, dict) else int(row[0])
            cur.execute(
                "INSERT INTO clientes (nombre_completo) VALUES (%s) ON CONFLICT (nombre_completo) DO NOTHING RETURNING id_cliente",
                (nombre_norm,),
            )
            r2 = cur.fetchone()
            if r2:
                conn.commit()
                return int(r2["id_cliente"]) if isinstance(r2, dict) else int(r2[0])
            # Conflicto: ya existía
            cur.execute("SELECT id_cliente FROM clientes WHERE nombre_completo=%s", (nombre_norm,))
            row = cur.fetchone()
            conn.commit()
            return int(row["id_cliente"]) if row else None


def db_insert_movimiento(
    id_cliente: int,
    fecha: Optional[str],
    tipo: str,
    monto: float,
    descripcion: Optional[str],
    docto: Optional[str],
    int_ag: Optional[str],
    dim: Optional[str],
    condicion_de_pago: Optional[str],
) -> bool:
    with db_connect() as conn:
        with conn.cursor() as cur:
            # calcular siguiente orden para el cliente (usar alias para dict_row)
            cur.execute("SELECT COALESCE(MAX(order_index), 0) + 1 AS next_order FROM movimientos WHERE id_cliente=%s", (id_cliente,))
            row = cur.fetchone()
            next_order = int(row["next_order"]) if isinstance(row, dict) else int(row[0])
            cur.execute(
                (
                    "INSERT INTO movimientos (id_cliente, fecha, tipo_de_movimiento, monto, descripcion, docto, int_ag, dim, condicion_de_pago, order_index) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT (id_cliente, fecha, tipo_de_movimiento, monto, descripcion) DO NOTHING RETURNING id_movimiento"
                ),
                (id_cliente, fecha, tipo, float(monto or 0), descripcion, docto, int_ag, dim, condicion_de_pago, next_order),
            )
            r = cur.fetchone()
            conn.commit()
            return bool(r)


