"""
Sistema Contable (Fase 1 - CLI local con JSON)

Requisitos previos:
  - Instale las dependencias con:
      pip install pandas openpyxl

Descripción general:
  - Aplicación de consola modular con Menú Principal y sub-menús.
  - Persistencia local en archivo JSON: estado_de_cuenta.json
  - Procesa un archivo Excel con columnas de entrada esperadas y lo transforma
    a un esquema de salida para Estado de Cuenta.

Arquitectura de módulos (Fase 1):
  - Menú Principal
    1) Gestionar Estado de Cuenta de Clientes
    2) Gestionar Planilla de Sueldos (futuro)
    3) Gestionar Comisiones (futuro)
    4) Gestionar Datos DIM (futuro)
    5) Salir

  - Sub-Menú Estado de Cuenta
    1) Cargar y Procesar Archivo Excel
    2) Ver Tabla de Estados de Cuenta
    3) Volver al Menú Principal

Notas:
  - El código está organizado en funciones para facilitar su futura migración
    a un backend (por ejemplo, Supabase) y añadir módulos sin romper lo existente.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from typing import Any, Dict, List

try:
    import pandas as pd
except ImportError as exc:  # Mensaje claro si faltan dependencias
    print("[ERROR] Faltan dependencias. Instale con: pip install pandas openpyxl")
    raise


# ==========================
# Configuración/Constantes
# ==========================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ESTADO_DE_CUENTA_JSON = os.path.join(BASE_DIR, "estado_de_cuenta.json")

# Columnas esperadas en el Excel de entrada
COLUMNAS_EXCEL_REQUERIDAS = [
    "CLIENTE",
    "FECHA",
    "SUC.",
    "DOC.",
    "NRO. FAC.",
    "NRO. DOC.",
    "CONCEPTO",
    "DEBITO",
    "CREDITO",
    "SALDO",
    "NRO. CBTE.",
    "TIP.",
    "TRAMITE",
    "DUI/DUE",
    "REFER.",
    "MERCADERIA",
]

# Columnas del esquema de salida (Estado de Cuenta consolidado)
COLUMNAS_SALIDA = [
    "FECHA",
    "DOCTO",
    "DETALLE",
    "DEBE",
    "HABER",
    "SALDO",
    "INT. AG.",
    "DIM",
    "CONDICION DE PAGO",
]


# ==========================
# Utilidades de consola/IO
# ==========================

def limpiar_pantalla() -> None:
    """Limpia la pantalla de la consola (Windows/Linux/Mac)."""
    comando = "cls" if os.name == "nt" else "clear"
    os.system(comando)


def pausar(mensaje: str = "Presione Enter para continuar...") -> None:
    """Pausa la ejecución esperando Enter del usuario."""
    try:
        input(mensaje)
    except EOFError:
        # En algunos entornos no interactivos input puede fallar; no bloquear
        pass


def leer_opcion_usuario(mensaje: str, opciones_validas: List[str]) -> str:
    """Lee una opción del usuario y valida contra una lista de opciones válidas."""
    while True:
        opcion = input(mensaje).strip()
        if opcion in opciones_validas:
            return opcion
        print(f"[!] Opción inválida. Opciones válidas: {', '.join(opciones_validas)}")


# ==========================
# Utilidades de datos
# ==========================

def cargar_json_estado_de_cuenta() -> List[Dict[str, Any]]:
    """Carga el JSON de estado de cuenta; si no existe, retorna lista vacía."""
    if not os.path.exists(ESTADO_DE_CUENTA_JSON):
        return []
    try:
        with open(ESTADO_DE_CUENTA_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
            # Si por algún motivo el archivo existe pero no es lista, lo normalizamos
            return []
    except json.JSONDecodeError:
        # Archivo corrupto o vacío no-JSON: normalizamos a lista vacía
        return []


def guardar_json_estado_de_cuenta(registros: List[Dict[str, Any]]) -> None:
    """Persiste la lista completa de registros al JSON."""
    with open(ESTADO_DE_CUENTA_JSON, "w", encoding="utf-8") as f:
        json.dump(registros, f, ensure_ascii=False, indent=2)


def validar_columnas_excel(df: pd.DataFrame) -> List[str]:
    """Retorna lista de columnas faltantes respecto a las requeridas."""
    faltantes = [col for col in COLUMNAS_EXCEL_REQUERIDAS if col not in df.columns]
    return faltantes


def normalizar_fecha(valor: Any) -> str:
    """Intenta parsear una fecha y devolver 'YYYY-MM-DD'. Si no es posible, devuelve str(valor)."""
    if pd.isna(valor):
        return ""
    # Si ya es Timestamp/fecha
    if isinstance(valor, (datetime,)):
        return valor.strftime("%Y-%m-%d")
    # Pandas puede traer tipo Timestamp
    if hasattr(valor, "to_pydatetime"):
        try:
            return valor.to_pydatetime().strftime("%Y-%m-%d")
        except Exception:
            pass
    # Intenta parsear distintos formatos comunes
    cadena = str(valor).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(cadena, fmt).strftime("%Y-%m-%d")
        except Exception:
            continue
    return cadena


def a_cadena_segura(valor: Any) -> str:
    """Convierte valores a str, evitando 'nan' y None."""
    if pd.isna(valor):
        return ""
    return str(valor).strip()


def a_numero_seguro(valor: Any) -> float:
    """Convierte a número float de forma tolerante.

    - Acepta strings con comas o puntos decimales.
    - Si no es convertible, retorna 0.0
    """
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
    # Normalizar coma decimal a punto
    texto = texto.replace(".", "").replace(",", ".") if texto.count(",") == 1 and texto.count(".") > 1 else texto
    # Intento directo
    try:
        return float(texto)
    except Exception:
        # Intento limpiando caracteres no numéricos (mantener - y .)
        limpio = "".join(ch for ch in texto if ch.isdigit() or ch in "-.")
        try:
            return float(limpio)
        except Exception:
            return 0.0


# ==========================
# Procesamiento de Excel
# ==========================

def procesar_excel(ruta_excel: str) -> List[Dict[str, Any]]:
    """Lee un Excel y transforma las filas al esquema de salida.

    Columnas de entrada esperadas:
      CLIENTE, FECHA, SUC., DOC., NRO. FAC., NRO. DOC., CONCEPTO, DEBITO,
      CREDITO, SALDO, NRO. CBTE., TIP., TRAMITE, DUI/DUE, REFER., MERCADERIA

    Esquema de salida por registro:
      FECHA, DOCTO (DOC. + NRO. FAC.), DETALLE (CONCEPTO + MERCADERIA),
      DEBE, HABER, SALDO, INT. AG., DIM, CONDICION DE PAGO
    """
    if not os.path.exists(ruta_excel):
        raise FileNotFoundError(f"No se encontró el archivo: {ruta_excel}")

    try:
        # Leemos sin forzar dtype para fechas; luego convertimos campos numéricos puntuales
        df = pd.read_excel(ruta_excel, engine="openpyxl")
    except Exception as exc:
        raise RuntimeError(f"No fue posible leer el Excel: {exc}") from exc

    faltantes = validar_columnas_excel(df)
    if faltantes:
        raise ValueError(
            "El archivo Excel no contiene todas las columnas requeridas. "
            f"Faltantes: {', '.join(faltantes)}"
        )

    registros_salida: List[Dict[str, Any]] = []
    for _, fila in df.iterrows():
        fecha = normalizar_fecha(fila.get("FECHA"))
        doc = a_cadena_segura(fila.get("DOC."))
        nro_fac = a_cadena_segura(fila.get("NRO. FAC."))
        concepto = a_cadena_segura(fila.get("CONCEPTO"))
        mercaderia = a_cadena_segura(fila.get("MERCADERIA"))
        debito = a_numero_seguro(fila.get("DEBITO"))
        credito = a_numero_seguro(fila.get("CREDITO"))
        saldo = a_numero_seguro(fila.get("SALDO"))

        docto = (doc + " "+ nro_fac).strip()
        detalle = (concepto + (" - " + mercaderia if mercaderia else "")).strip()

        registro = {
            "FECHA": fecha,
            "DOCTO": docto,
            "DETALLE": detalle,
            "DEBE": round(debito, 2),
            "HABER": round(credito, 2),
            "SALDO": round(saldo, 2),
            "INT. AG.": "",
            "DIM": "",
            "CONDICION DE PAGO": "",
        }
        registros_salida.append(registro)

    return registros_salida


# ==========================
# Presentación de datos
# ==========================

def mostrar_tabla_estado_de_cuenta() -> None:
    """Lee el JSON y muestra una tabla formateada en consola."""
    registros = cargar_json_estado_de_cuenta()
    if not registros:
        print("[i] No hay registros en estado_de_cuenta.json aún.")
        return

    # Normalizamos a DataFrame con columnas en el orden esperado
    df = pd.DataFrame(registros)
    # Asegurar columnas y orden
    for col in COLUMNAS_SALIDA:
        if col not in df.columns:
            df[col] = ""
    df = df[COLUMNAS_SALIDA]

    # Opciones de presentación
    with pd.option_context(
        "display.max_columns",
        None,
        "display.width",
        140,
        "display.colheader_justify",
        "center",
        "display.max_colwidth",
        40,
    ):
        print("\nEstado de Cuenta (consolidado):\n")
        print(df.to_string(index=False))


# ==========================
# Menús y Control de Flujo
# ==========================

def mostrar_menu_principal() -> None:
    print("=" * 60)
    print("            SISTEMA CONTABLE - MENÚ PRINCIPAL            ")
    print("=" * 60)
    print("1) Gestionar Estado de Cuenta de Clientes")
    print("2) Gestionar Planilla de Sueldos (próximamente)")
    print("3) Gestionar Comisiones (próximamente)")
    print("4) Gestionar Datos DIM (próximamente)")
    print("5) Salir")


def mostrar_submenu_estado_cuenta() -> None:
    print("-" * 60)
    print("        SUB-MENÚ: ESTADO DE CUENTA DE CLIENTES          ")
    print("-" * 60)
    print("1) Cargar y Procesar Archivo Excel")
    print("2) Ver Tabla de Estados de Cuenta")
    print("3) Volver al Menú Principal")


def gestionar_estado_de_cuenta() -> None:
    """Loop del sub-menú de Estado de Cuenta."""
    while True:
        limpiar_pantalla()
        mostrar_submenu_estado_cuenta()
        opcion = leer_opcion_usuario("Seleccione una opción (1-3): ", ["1", "2", "3"])

        if opcion == "1":
            # Cargar y procesar Excel
            ruta = input("Ingrese la ruta del archivo Excel (.xlsx): ").strip().strip('"')
            if not ruta:
                print("[!] Ruta no proporcionada.")
                pausar()
                continue
            try:
                nuevos_registros = procesar_excel(ruta)
                existentes = cargar_json_estado_de_cuenta()
                combinados = existentes + nuevos_registros
                guardar_json_estado_de_cuenta(combinados)
                print(f"[+] {len(nuevos_registros)} registros procesados y guardados correctamente.")
            except Exception as exc:
                print(f"[ERROR] {exc}")
            pausar()

        elif opcion == "2":
            # Ver tabla
            limpiar_pantalla()
            mostrar_tabla_estado_de_cuenta()
            pausar()

        elif opcion == "3":
            # Volver al Menú Principal
            break


def gestionar_planilla() -> None:
    print("[+] El módulo 'Planilla de Sueldos' se implementará próximamente.")


def gestionar_comisiones() -> None:
    print("[+] El módulo 'Comisiones' se implementará próximamente.")


def gestionar_datos_dim() -> None:
    print("[+] El módulo 'Datos DIM' se implementará próximamente.")


def main() -> None:
    # Mensaje inicial sobre dependencias (también hay verificación al importar)
    print("Dependencias requeridas: pandas y openpyxl (pip install pandas openpyxl)")

    while True:
        try:
            limpiar_pantalla()
            mostrar_menu_principal()
            opcion = leer_opcion_usuario("Seleccione una opción (1-5): ", ["1", "2", "3", "4", "5"])

            if opcion == "1":
                limpiar_pantalla()
                gestionar_estado_de_cuenta()
            elif opcion == "2":
                limpiar_pantalla()
                gestionar_planilla()
                pausar()
            elif opcion == "3":
                limpiar_pantalla()
                gestionar_comisiones()
                pausar()
            elif opcion == "4":
                limpiar_pantalla()
                gestionar_datos_dim()
                pausar()
            elif opcion == "5":
                print("Saliendo del sistema. ¡Hasta luego!")
                break

        except KeyboardInterrupt:
            print("\n[!] Interrupción por usuario. Saliendo...")
            break


if __name__ == "__main__":
    main()


