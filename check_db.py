from __future__ import annotations

import os
import sys

try:
    import psycopg
except Exception as exc:
    print("[ERROR] Falta psycopg 3.x. Ejecute: pip install -r requirements.txt")
    sys.exit(1)


def main() -> None:
    host = os.environ.get("PGHOST", "localhost")
    port = int(os.environ.get("PGPORT", "5432"))
    db = os.environ.get("PGDATABASE")
    user = os.environ.get("PGUSER")
    pwd = os.environ.get("PGPASSWORD")
    if not (db and user and pwd):
        print("[ERROR] Faltan PGDATABASE/PGUSER/PGPASSWORD en variables de entorno")
        sys.exit(1)

    # psycopg3 maneja mejor la codificación por defecto
    with psycopg.connect(host=host, port=port, dbname=db, user=user, password=pwd) as conn:
        with conn.cursor() as cur:
            # Existencia de tablas
            cur.execute("""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_schema='public' AND table_name IN ('clientes','movimientos')
            """)
            print("tablas_public_clientes_movimientos=", cur.fetchone()[0])

            # Conteos
            cur.execute("SELECT COUNT(*) FROM clientes")
            print("clientes=", cur.fetchone()[0])
            cur.execute("SELECT COUNT(*) FROM movimientos")
            print("movimientos=", cur.fetchone()[0])

            # Top 10 por cliente
            cur.execute(
                """
                SELECT c.nombre_completo, COUNT(*) AS n
                FROM movimientos m 
                JOIN clientes c ON m.id_cliente=c.id_cliente 
                GROUP BY 1 ORDER BY 2 DESC LIMIT 10
                """
            )
            rows = cur.fetchall()
            print("top10=", rows)


if __name__ == "__main__":
    main()


