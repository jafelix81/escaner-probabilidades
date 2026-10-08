import numpy as np
import pandas as pd
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib.request
import time
from datetime import datetime, timezone


# ============================================================
# CERE v1.1 — MOTOR DE DATOS ROBUSTO
# ============================================================
#
# OBJETIVO:
#   Construir el estado estadístico actual de cada ticker.
#
# NO HACE TODAVÍA:
#   - selección de compras
#   - Kelly
#   - bootstrap
#   - vecinos históricos
#   - Student-t condicional
#   - EV
#
# Eso llegará en CERE v2.
#
# MEJORA v1.1:
#   - Descarga masiva inicial.
#   - Validación individual.
#   - Retry automático para cualquier ticker
#     que presente problemas de datos.
#   - Máximo 2 reintentos individuales.
#   - Registro de DATA_ATTEMPTS.
#   - Registro de DATA_SOURCE.
#   - Diagnóstico de tickers inactivos/delisted.
#   - Registro de número de observaciones.
#   - Registro de primera y última fecha disponible.
#
# FLUJO:
#
# Yahoo Finance
#      ↓
# Descarga masiva
#      ↓
# Validación individual
#      ↓
# ¿Problema?
#      ↓
# Retry individual
#      ↓
# Diagnóstico
#      ↓
# OK / DELISTED / INACTIVE / INSUFFICIENT / ERROR
#      ↓
# Estado estadístico actual
#      ↓
# Google Apps Script
#      ↓
# CERE_CURRENT
# CERE_HISTORY
# CERE_STATUS
#
# ============================================================


print("=" * 70)
print("🚀 CERE v1.1 — MOTOR DE DATOS ROBUSTO")
print("=" * 70)


# ============================================================
# 1. CONFIGURACIÓN
# ============================================================

URL_RECEPTORA_GOOGLE = (
    "https://script.google.com/macros/s/"
    "AKfycbyKFg59lbEzKboZqw5N09GCnMxFrIs4Xb_eq6HUjl87ib87pjvbm4I2T5FALvUdEKe3/"
    "exec"
)


# ------------------------------------------------------------
# UNIVERSO
# ------------------------------------------------------------

universo_tickers = [
    "AAPL", "NVDA", "AMD", "MSFT", "GOOGL", "AMZN", "META", "TSLA",
    "INTC", "QCOM", "AVGO", "NFLX", "CSCO", "AMAT", "MU",
    "PANW", "SNPS", "CDNS", "PLTR", "PYPL", "SHOP", "NET", "DDOG",
    "CRWD", "OKTA", "ZS", "MDB", "TEAM", "WDAY",
    "NOW", "SNOW", "ZM", "DOCU", "ROKU", "TWLO", "PINS", "SNAP",
    "MTCH", "TSM", "ASML", "LRCX", "KLAC", "NXPI", "TXN",
    "ADI", "MCHP", "ON", "MRVL", "TER", "ENPH", "SEDG", "FSLR",
    "FLEX", "COIN", "MARA", "RIOT", "SOFI", "AFRM", "UPST",
    "HOOD", "DKNG", "NU", "MELI", "SE", "V", "MA", "AXP", "REGN",
    "BIIB", "GILD", "AMGN", "VRTX", "ILMN", "ALGN",
    "MRNA", "BNTX", "CRSP", "EDIT", "NTLA", "BEAM", "SBUX", "MDLZ",
    "CHTR", "TMUS", "CMCSA", "EA", "TTWO", "ABNB",
    "BKNG", "EXPE", "TRIP", "PDD", "JD", "BABA", "BIDU", "NIO",
    "LI", "XPEV", "LCID", "RIVN", "QS", "PLUG",
    "RUN", "CHPT", "BLNK", "BE", "FCEL", "SPWR", "CAT", "DE", "HON",
    "GE", "MMM", "LMT", "BA", "NOC",
    "GD", "RTX", "UPS", "FDX", "CSX", "NSC", "UNP", "WM", "RSG",
    "JPM", "BAC", "WFC", "C", "GS",
    "MS", "BLK", "BX", "KKR", "APO", "TROW", "BEN", "STT", "NTRS",
    "SCHW", "AMTD", "WMT", "TGT",
    "COST", "HD", "LOW", "PG", "KO", "PEP", "EL", "CL", "KMB", "GIS",
    "MNST", "CELH", "XOM",
    "CVX", "COP", "EOG", "SLB", "HAL", "BKR", "OXY", "DVN", "APA",
    "FCX", "NEM", "NUE",
    "STLD", "AA", "CLF", "T", "VZ", "DIS", "WBD", "PARA", "FOXA",
    "NWSA", "LYV", "SIFY",
    "VOD", "TELFY", "LIN", "APD", "ECL", "SHW", "DD", "DOW", "MOS",
    "CF", "NTR", "FMC",
    "VMC", "MLM", "CX", "LEN", "DHI", "PHM", "PLD", "AMT", "CCI",
    "EQIX", "O", "SPG",
    "PSA", "EXR", "AVB", "EQR", "MAA", "VICI", "DLR", "SBAC", "WY",
    "BXP", "ISRG", "SYK",
    "ZBH", "EW", "BSX", "MDT", "ABT", "BMY", "PFE", "JNJ", "LLY",
    "NVO", "AZN", "SNY",
    "GSK", "TAK", "Z", "ZG", "OPEN", "COMP", "W", "CVNA", "CHWY",
    "JMIA", "EBAY", "ETSY",
    "WAY", "PAGS", "STNE", "PAYS", "FLYW", "LOT", "LPRO", "OPFI",
    "CACC", "OMF", "FCF",
    "GPRO", "FITB", "HBAN", "KEY", "RF", "CFG", "MTB", "ZION",
    "TFC", "AX", "CUBI", "HOMB",
    "OZK", "FNB", "ASB", "VLY", "UMBF", "BOKF", "EGBN", "WBS",
    "CATY", "IWM", "QQQ",
    "VEA", "VWO", "IEFA", "EEM", "VNQ", "GLD", "USO", "UNG", "OIH",
    "XLE", "XLF", "XLK",
    "XLV", "XLY", "XLP", "XLI", "XLB", "XLU", "XLRE", "SMH", "SOXX",
    "XBI", "KRE", "JETS",
    "ARKK", "ARKW", "ARKG", "ARKF", "ARKQ", "BITO", "RUM", "DJT",
    "PSNY", "MP", "SMCI",
    "DELL", "ANET", "VRT", "LITE", "CLS", "MOD", "CRDO", "ALAB",
    "GLW", "CEG", "VST",
    "GEV", "ETN", "PWR", "APP", "AXON", "RDDT", "DUOL", "CAVA",
    "TEM", "TMDX", "IREN",
    "RKLB", "ASTS", "HUT", "WULF", "CIFR", "ATI", "XYZ", "HAPN"
]

universo_tickers = sorted(set(universo_tickers))

print(
    f"📊 Universo total configurado: "
    f"{len(universo_tickers)} tickers"
)


# ============================================================
# 2. CONFIGURACIÓN DE HISTORIA
# ============================================================

PERIODO_HISTORICO = "10y"

MINIMO_DATOS = 260

# Número máximo de reintentos individuales.
MAX_REINTENTOS_INDIVIDUALES = 2

# Pausa entre reintentos.
ESPERA_RETRY_SEGUNDOS = 2

# Mínimo de observaciones que consideraremos como
# evidencia de que el ticker tiene algo de historial,
# pero no suficiente para el modelo.
MINIMO_HISTORIA_DETECTABLE = 20


# ============================================================
# 3. GENERAR RUN ID
# ============================================================

ahora_utc = datetime.now(timezone.utc)

run_id = ahora_utc.strftime("%Y%m%d_%H%M%S")

timestamp_utc = ahora_utc.strftime(
    "%Y-%m-%dT%H:%M:%SZ"
)

print(
    f"🆔 RUN_ID: {run_id}"
)

print(
    f"🕐 Timestamp UTC: {timestamp_utc}"
)


# ============================================================
# 4. DESCARGA HISTÓRICA MASIVA
# ============================================================

print()

print(
    "📥 Descargando aproximadamente 10 años "
    "de historia diaria..."
)

inicio_descarga = time.time()

try:

    data_descarga = yf.download(
        universo_tickers,
        period=PERIODO_HISTORICO,
        interval="1d",
        auto_adjust=True,
        progress=False,
        group_by="column",
        threads=True
    )

except Exception as e:

    print(
        f"❌ ERROR CRÍTICO descargando Yahoo Finance: {e}"
    )

    raise


tiempo_descarga = (
    time.time()
    - inicio_descarga
)

print(
    f"✅ Descarga masiva terminada en "
    f"{tiempo_descarga:.1f} segundos"
)


# ============================================================
# 5. OBTENER CLOSE DESDE DESCARGA MASIVA
# ============================================================

def obtener_close_bulk(ticker):

    try:

        if (
            data_descarga is None
            or data_descarga.empty
        ):
            return None

        if isinstance(
            data_descarga.columns,
            pd.MultiIndex
        ):

            if (
                "Close"
                not in data_descarga.columns.levels[0]
            ):
                return None

            if (
                ticker
                not in data_descarga["Close"].columns
            ):
                return None

            serie = data_descarga["Close"][ticker]

        else:

            if "Close" not in data_descarga.columns:
                return None

            serie = data_descarga["Close"]

        serie = pd.to_numeric(
            serie,
            errors="coerce"
        ).dropna()

        if len(serie) < MINIMO_DATOS:
            return None

        return serie

    except Exception:

        return None


# ============================================================
# 6. RETRY INDIVIDUAL + DIAGNÓSTICO DE DATOS
# ============================================================

def obtener_close_individual(ticker):

    ultimo_error = ""

    ultima_serie = None

    ultimo_numero_observaciones = 0

    for intento in range(
        1,
        MAX_REINTENTOS_INDIVIDUALES + 1
    ):

        try:

            print(
                f"🔄 Retry individual "
                f"{ticker} — intento "
                f"{intento}/{MAX_REINTENTOS_INDIVIDUALES}"
            )

            datos = yf.download(
                ticker,
                period=PERIODO_HISTORICO,
                interval="1d",
                auto_adjust=True,
                progress=False,
                threads=False
            )

            if datos is None or datos.empty:

                ultimo_error = (
                    "Yahoo devolvió datos vacíos."
                )

            else:

                serie = None

                if isinstance(
                    datos.columns,
                    pd.MultiIndex
                ):

                    if (
                        "Close"
                        not in datos.columns.levels[0]
                    ):

                        ultimo_error = (
                            "No se encontró columna Close."
                        )

                    else:

                        serie = datos["Close"]

                        if isinstance(
                            serie,
                            pd.DataFrame
                        ):

                            if ticker in serie.columns:

                                serie = serie[ticker]

                            else:

                                serie = serie.iloc[:, 0]

                else:

                    if "Close" not in datos.columns:

                        ultimo_error = (
                            "No se encontró columna Close."
                        )

                    else:

                        serie = datos["Close"]

                if serie is not None:

                    serie = pd.to_numeric(
                        serie,
                        errors="coerce"
                    ).dropna()

                    ultimo_numero_observaciones = (
                        len(serie)
                    )

                    ultima_serie = serie

                    if (
                        len(serie)
                        >= MINIMO_DATOS
                    ):

                        return {
                            "serie": serie,
                            "partial_serie": serie,
                            "attempts": (
                                MAX_REINTENTOS_INDIVIDUALES
                                + 1
                            ),
                            "source": "INDIVIDUAL_RETRY",
                            "error": "",
                            "observaciones": len(serie)
                        }

                    ultimo_error = (
                        f"Solo {len(serie)} "
                        "observaciones válidas."
                    )

        except Exception as e:

            ultimo_error = (
                f"{type(e).__name__}: "
                f"{str(e)}"
            )

        if intento < MAX_REINTENTOS_INDIVIDUALES:

            time.sleep(
                ESPERA_RETRY_SEGUNDOS
            )

    return {
        "serie": None,
        "partial_serie": ultima_serie,
        "attempts": (
            MAX_REINTENTOS_INDIVIDUALES + 1
        ),
        "source": "FAILED",
        "error": ultimo_error,
        "observaciones": ultimo_numero_observaciones
    }


# ============================================================
# 6B. DIAGNÓSTICO DE TICKER INACTIVO / DELISTED
# ============================================================

def diagnosticar_ticker_inactivo(
    ticker,
    serie_parcial=None,
    error_yahoo=""
):

    observaciones = 0

    fecha_inicio = ""

    fecha_fin = ""

    if serie_parcial is not None:

        try:

            serie_parcial = pd.to_numeric(
                serie_parcial,
                errors="coerce"
            ).dropna()

            observaciones = len(
                serie_parcial
            )

            if not serie_parcial.empty:

                fecha_inicio = (
                    serie_parcial.index.min()
                    .strftime("%Y-%m-%d")
                )

                fecha_fin = (
                    serie_parcial.index.max()
                    .strftime("%Y-%m-%d")
                )

        except Exception:

            pass

    # --------------------------------------------------------
    # Muy pocas observaciones:
    # fuerte señal de símbolo inactivo/delisted.
    # --------------------------------------------------------

    if observaciones > 0:

        if (
            observaciones
            < MINIMO_HISTORIA_DETECTABLE
        ):

            return {
                "status": "DELISTED",

                "diagnosis": (
                    "Yahoo devolvió únicamente "
                    f"{observaciones} observaciones. "
                    "El símbolo parece inactivo, "
                    "delistado o dejó de cotizar."
                ),

                "observaciones": observaciones,

                "fecha_inicio": fecha_inicio,

                "fecha_fin": fecha_fin
            }

    # --------------------------------------------------------
    # Cero observaciones:
    # no afirmamos automáticamente DELISTED.
    # Puede ser un problema temporal de Yahoo.
    # --------------------------------------------------------

    if observaciones == 0:

        return {
            "status": "INACTIVE_OR_UNAVAILABLE",

            "diagnosis": (
                "Yahoo no devolvió historial válido "
                "para el símbolo después de los "
                "reintentos individuales."
            ),

            "observaciones": 0,

            "fecha_inicio": "",

            "fecha_fin": ""
        }

    # --------------------------------------------------------
    # Existe historial, pero es insuficiente.
    # --------------------------------------------------------

    return {
        "status": "INSUFFICIENT_DATA",

        "diagnosis": (
            f"Solo {observaciones} "
            "observaciones válidas; "
            f"se requieren {MINIMO_DATOS}."
        ),

        "observaciones": observaciones,

        "fecha_inicio": fecha_inicio,

        "fecha_fin": fecha_fin
    }


# ============================================================
# 7. OBTENER HISTORIA CON FALLBACK AUTOMÁTICO
# ============================================================

def obtener_historia_robusta(ticker):

    # --------------------------------------------------------
    # INTENTO 1: descarga masiva
    # --------------------------------------------------------

    serie = obtener_close_bulk(
        ticker
    )

    if serie is not None:

        return {
            "serie": serie,

            "attempts": 1,

            "source": "BULK",

            "status": "OK",

            "error": "",

            "diagnosis": "",

            "observaciones": len(serie),

            "fecha_inicio": (
                serie.index.min()
                .strftime("%Y-%m-%d")
            ),

            "fecha_fin": (
                serie.index.max()
                .strftime("%Y-%m-%d")
            )
        }

    # --------------------------------------------------------
    # Si la descarga masiva falló,
    # hacemos retry individual.
    # --------------------------------------------------------

    retry = obtener_close_individual(
        ticker
    )

    if retry["serie"] is not None:

        serie = retry["serie"]

        return {
            "serie": serie,

            "attempts": retry["attempts"],

            "source": retry["source"],

            "status": "OK",

            "error": "",

            "diagnosis": "",

            "observaciones": len(serie),

            "fecha_inicio": (
                serie.index.min()
                .strftime("%Y-%m-%d")
            ),

            "fecha_fin": (
                serie.index.max()
                .strftime("%Y-%m-%d")
            )
        }

    # --------------------------------------------------------
    # No se recuperó.
    # Diagnosticar la causa.
    # --------------------------------------------------------

    diagnostico = diagnosticar_ticker_inactivo(
        ticker=ticker,

        serie_parcial=retry.get(
            "partial_serie",
            None
        ),

        error_yahoo=retry.get(
            "error",
            ""
        )
    )

    return {
        "serie": None,

        "attempts": retry["attempts"],

        "source": retry["source"],

        "status": diagnostico["status"],

        "error": diagnostico["diagnosis"],

        "diagnosis": diagnostico["diagnosis"],

        "observaciones": diagnostico[
            "observaciones"
        ],

        "fecha_inicio": diagnostico[
            "fecha_inicio"
        ],

        "fecha_fin": diagnostico[
            "fecha_fin"
        ]
    }


# ============================================================
# 8. FUNCIONES ESTADÍSTICAS
# ============================================================

def retorno_simple(
    precios,
    n
):

    if len(precios) <= n:

        return np.nan

    anterior = precios[
        -n - 1
    ]

    actual = precios[-1]

    if anterior <= 0:

        return np.nan

    return (
        actual / anterior
    ) - 1.0


def volatilidad_anualizada(
    retornos_log,
    ventana
):

    if len(retornos_log) < ventana:

        return np.nan

    ventana_ret = retornos_log[
        -ventana:
    ]

    if len(ventana_ret) < 2:

        return np.nan

    vol = (
        np.std(
            ventana_ret,
            ddof=1
        )
        * np.sqrt(252)
    )

    return float(vol)


def skewness(
    retornos,
    ventana
):

    if len(retornos) < ventana:

        return np.nan

    serie = pd.Series(
        retornos[-ventana:]
    )

    return float(
        serie.skew()
    )


def kurtosis_excess(
    retornos,
    ventana
):

    if len(retornos) < ventana:

        return np.nan

    serie = pd.Series(
        retornos[-ventana:]
    )

    # Fisher=True:
    # distribución normal ≈ 0

    return float(
        serie.kurt()
    )


def autocorrelacion_1(
    retornos,
    ventana=60
):

    if len(retornos) < ventana + 1:

        return np.nan

    serie = pd.Series(
        retornos[-ventana:]
    )

    valor = serie.autocorr(
        lag=1
    )

    return (
        float(valor)
        if pd.notna(valor)
        else np.nan
    )


# ============================================================
# 9. PRECIO INTRADÍA ACTUAL
# ============================================================

def obtener_precio_actual(
    ticker,
    precio_cierre
):

    try:

        intradia = yf.download(
            ticker,
            period="1d",
            interval="1m",
            auto_adjust=False,
            progress=False,
            threads=False
        )

        if (
            intradia is not None
            and not intradia.empty
        ):

            if isinstance(
                intradia.columns,
                pd.MultiIndex
            ):

                if (
                    "Close"
                    in intradia.columns.levels[0]
                ):

                    serie = intradia[
                        "Close"
                    ]

                    if isinstance(
                        serie,
                        pd.DataFrame
                    ):

                        serie = serie.iloc[
                            :,
                            0
                        ]

                else:

                    serie = None

            else:

                if "Close" in intradia.columns:

                    serie = intradia[
                        "Close"
                    ]

                else:

                    serie = None

            if serie is not None:

                serie = pd.to_numeric(
                    serie,
                    errors="coerce"
                ).dropna()

                if not serie.empty:

                    precio = float(
                        serie.iloc[-1]
                    )

                    if (
                        np.isfinite(precio)
                        and precio > 0
                    ):

                        return precio

    except Exception:

        pass

    # Fallback al último cierre ajustado.

    return float(
        precio_cierre
    )


# ============================================================
# 10. ANÁLISIS DE UN TICKER
# ============================================================

def analizar_ticker(
    ticker
):

    resultado_base = {

        "Ticker": ticker,

        "STATUS": "ERROR",

        "ERROR": "",

        "DATA_ATTEMPTS": 0,

        "DATA_SOURCE": "",

        "DATA_OBSERVATIONS": 0,

        "DATA_FIRST_DATE": "",

        "DATA_LAST_DATE": "",

        "DATA_DIAGNOSIS": ""
    }

    try:

        # ----------------------------------------------------
        # HISTORIA ROBUSTA
        # ----------------------------------------------------

        historia = obtener_historia_robusta(
            ticker
        )

        precios_serie = historia[
            "serie"
        ]

        resultado_base[
            "DATA_ATTEMPTS"
        ] = historia[
            "attempts"
        ]

        resultado_base[
            "DATA_SOURCE"
        ] = historia[
            "source"
        ]

        resultado_base[
            "DATA_OBSERVATIONS"
        ] = historia.get(
            "observaciones",
            0
        )

        resultado_base[
            "DATA_FIRST_DATE"
        ] = historia.get(
            "fecha_inicio",
            ""
        )

        resultado_base[
            "DATA_LAST_DATE"
        ] = historia.get(
            "fecha_fin",
            ""
        )

        resultado_base[
            "DATA_DIAGNOSIS"
        ] = historia.get(
            "diagnosis",
            ""
        )

        if precios_serie is None:

            resultado_base[
                "STATUS"
            ] = historia.get(
                "status",
                "ERROR"
            )

            resultado_base[
                "ERROR"
            ] = historia.get(
                "error",
                ""
            )

            return resultado_base

        precios = precios_serie.to_numpy(
            dtype=float
        )

        if len(precios) < MINIMO_DATOS:

            resultado_base[
                "STATUS"
            ] = "INSUFFICIENT_DATA"

            resultado_base[
                "ERROR"
            ] = (
                f"Solo {len(precios)} "
                f"observaciones válidas; "
                f"se requieren "
                f"{MINIMO_DATOS}."
            )

            resultado_base[
                "DATA_OBSERVATIONS"
            ] = len(precios)

            return resultado_base

        # ----------------------------------------------------
        # RETORNOS LOG
        # ----------------------------------------------------

        retornos_log = np.diff(
            np.log(precios)
        )

        # ----------------------------------------------------
        # RETORNOS SIMPLES
        # ----------------------------------------------------

        r1 = retorno_simple(
            precios,
            1
        )

        r5 = retorno_simple(
            precios,
            5
        )

        r20 = retorno_simple(
            precios,
            20
        )

        r60 = retorno_simple(
            precios,
            60
        )

        r120 = retorno_simple(
            precios,
            120
        )

        # ----------------------------------------------------
        # VOLATILIDAD
        # ----------------------------------------------------

        vol5 = volatilidad_anualizada(
            retornos_log,
            5
        )

        vol20 = volatilidad_anualizada(
            retornos_log,
            20
        )

        vol60 = volatilidad_anualizada(
            retornos_log,
            60
        )

        vol252 = volatilidad_anualizada(
            retornos_log,
            252
        )

        # ----------------------------------------------------
        # RATIOS DE VOLATILIDAD
        # ----------------------------------------------------

        if (
            pd.notna(vol20)
            and pd.notna(vol252)
            and vol252 > 0
        ):

            vol_ratio20 = (
                vol20 / vol252
            )

        else:

            vol_ratio20 = np.nan

        if (
            pd.notna(vol60)
            and pd.notna(vol252)
            and vol252 > 0
        ):

            vol_ratio60 = (
                vol60 / vol252
            )

        else:

            vol_ratio60 = np.nan

        # ----------------------------------------------------
        # FORMA DE DISTRIBUCIÓN
        # ----------------------------------------------------

        skew20 = skewness(
            retornos_log,
            20
        )

        kurt20 = kurtosis_excess(
            retornos_log,
            20
        )

        # ----------------------------------------------------
        # AUTOCORRELACIÓN
        # ----------------------------------------------------

        ac1 = autocorrelacion_1(
            retornos_log,
            60
        )

        # ----------------------------------------------------
        # PRECIO DE REFERENCIA
        # ----------------------------------------------------

        precio_cierre = float(
            precios[-1]
        )

        precio_actual = obtener_precio_actual(
            ticker,
            precio_cierre
        )

        # ----------------------------------------------------
        # VALIDACIÓN
        # ----------------------------------------------------

        variables = [

            r1,

            r5,

            r20,

            r60,

            r120,

            vol5,

            vol20,

            vol60,

            vol252,

            vol_ratio20,

            vol_ratio60,

            skew20,

            kurt20,

            ac1
        ]

        cantidad_validas = sum(

            pd.notna(x)
            and np.isfinite(x)

            for x in variables
        )

        if cantidad_validas < 12:

            resultado_base[
                "STATUS"
            ] = "INSUFFICIENT_FEATURES"

            resultado_base[
                "ERROR"
            ] = (
                f"Solo {cantidad_validas}/14 "
                "variables estadísticas válidas."
            )

            return resultado_base

        # ----------------------------------------------------
        # RESULTADO
        # ----------------------------------------------------

        resultado = {

            "RUN_ID": run_id,

            "CAPTURED_AT_UTC": timestamp_utc,

            "Ticker": ticker,

            "STATUS": "OK",

            "ERROR": "",

            "DATA_ATTEMPTS": historia[
                "attempts"
            ],

            "DATA_SOURCE": historia[
                "source"
            ],

            "DATA_OBSERVATIONS": historia.get(
                "observaciones",
                len(precios)
            ),

            "DATA_FIRST_DATE": historia.get(
                "fecha_inicio",
                ""
            ),

            "DATA_LAST_DATE": historia.get(
                "fecha_fin",
                ""
            ),

            "DATA_DIAGNOSIS": historia.get(
                "diagnosis",
                ""
            ),

            "Price_Actual": precio_actual,

            "Price_Close": precio_cierre,

            "R1": r1,

            "R5": r5,

            "R20": r20,

            "R60": r60,

            "R120": r120,

            "Vol5": vol5,

            "Vol20": vol20,

            "Vol60": vol60,

            "Vol252": vol252,

            "VolRatio20": vol_ratio20,

            "VolRatio60": vol_ratio60,

            "Skew20": skew20,

            "Kurt20": kurt20,

            "AC1": ac1,

            # ------------------------------------------------
            # Reservado para CERE v2.
            # TODAVÍA NO SON CÁLCULOS DE EV.
            # ------------------------------------------------

            "Forward5": np.nan,

            "Forward10": np.nan,

            "Forward20": np.nan,

            "Forward40": np.nan,

            "EV5": np.nan,

            "EV10": np.nan,

            "EV20": np.nan,

            "EV40": np.nan,

            "Confidence5": np.nan,

            "Confidence10": np.nan,

            "Confidence20": np.nan,

            "Confidence40": np.nan,

            "ESS5": np.nan,

            "ESS10": np.nan,

            "ESS20": np.nan,

            "ESS40": np.nan,

            "ES95_5": np.nan,

            "ES95_10": np.nan,

            "ES95_20": np.nan,

            "ES95_40": np.nan,

            "Kelly25": np.nan
        }

        return resultado

    except Exception as e:

        resultado_base[
            "STATUS"
        ] = "ERROR"

        resultado_base[
            "ERROR"
        ] = (
            f"{type(e).__name__}: "
            f"{str(e)}"
        )

        return resultado_base


# ============================================================
# 11. EJECUCIÓN PARA TODO EL UNIVERSO
# ============================================================

print()

print(
    "⚙️ Calculando estados estadísticos..."
)

inicio_calculo = time.time()

resultados = []

MAX_WORKERS = 12

with ThreadPoolExecutor(
    max_workers=MAX_WORKERS
) as executor:

    futures = {

        executor.submit(
            analizar_ticker,
            ticker
        ): ticker

        for ticker in universo_tickers
    }

    for future in as_completed(
        futures
    ):

        ticker = futures[
            future
        ]

        try:

            resultado = future.result()

            if resultado is not None:

                resultados.append(
                    resultado
                )

        except Exception as e:

            resultados.append({

                "Ticker": ticker,

                "STATUS": "ERROR",

                "ERROR": (
                    f"{type(e).__name__}: "
                    f"{str(e)}"
                ),

                "DATA_ATTEMPTS": 0,

                "DATA_SOURCE": "FAILED",

                "DATA_OBSERVATIONS": 0,

                "DATA_FIRST_DATE": "",

                "DATA_LAST_DATE": "",

                "DATA_DIAGNOSIS": ""
            })


tiempo_calculo = (
    time.time()
    - inicio_calculo
)

print(
    f"✅ Cálculo terminado en "
    f"{tiempo_calculo:.1f} segundos"
)


# ============================================================
# 12. ORDENAR RESULTADOS
# ============================================================

df_resultados = pd.DataFrame(
    resultados
)

if df_resultados.empty:

    raise RuntimeError(
        "No se produjo ningún resultado."
    )


if "Ticker" in df_resultados.columns:

    df_resultados = (

        df_resultados

        .sort_values(
            "Ticker"
        )

        .reset_index(
            drop=True
        )
    )


# ============================================================
# 13. VALIDACIÓN DE INTEGRIDAD
# ============================================================

total_universo = len(
    universo_tickers
)

total_procesados = len(
    df_resultados
)

total_ok = int(
    (
        df_resultados["STATUS"]
        == "OK"
    ).sum()
)

total_delisted = int(
    (
        df_resultados["STATUS"]
        == "DELISTED"
    ).sum()
)

total_inactive = int(
    (
        df_resultados["STATUS"]
        == "INACTIVE_OR_UNAVAILABLE"
    ).sum()
)

total_insufficient = int(
    (
        df_resultados["STATUS"]
        == "INSUFFICIENT_DATA"
    ).sum()
)

total_errors = int(
    (
        df_resultados["STATUS"]
        == "ERROR"
    ).sum()
)

total_error = (
    total_procesados
    - total_ok
)


print()
print("=" * 70)
print("🔍 AUDITORÍA DE INTEGRIDAD")
print("=" * 70)

print(
    f"Universo configurado : "
    f"{total_universo}"
)

print(
    f"Tickers procesados   : "
    f"{total_procesados}"
)

print(
    f"Tickers OK            : "
    f"{total_ok}"
)

print(
    f"Tickers con problema  : "
    f"{total_error}"
)

print(
    f"  ├─ DELISTED         : "
    f"{total_delisted}"
)

print(
    f"  ├─ INACTIVE         : "
    f"{total_inactive}"
)

print(
    f"  ├─ INSUFFICIENT     : "
    f"{total_insufficient}"
)

print(
    f"  └─ ERROR            : "
    f"{total_errors}"
)


# ------------------------------------------------------------
# Regla de seguridad
# ------------------------------------------------------------

if total_procesados < int(
    total_universo * 0.90
):

    raise RuntimeError(

        "FALLO DE INTEGRIDAD: "
        "menos del 90% del universo "
        "fue procesado. "
        "NO se enviarán datos a "
        "Google Sheets."
    )


if total_ok < int(
    total_universo * 0.80
):

    raise RuntimeError(

        "FALLO DE INTEGRIDAD: "
        "menos del 80% de los tickers "
        "tienen estado OK. "
        "NO se enviarán datos a "
        "Google Sheets."
    )


# ============================================================
# 14. RESULTADO DE RETRIES Y DIAGNÓSTICO
# ============================================================

print()
print("=" * 70)
print("🔄 RESULTADO DE RETRIES Y DIAGNÓSTICO")
print("=" * 70)

if "DATA_SOURCE" in df_resultados.columns:

    # --------------------------------------------------------
    # Tickers que necesitaron retry
    # --------------------------------------------------------

    retries = df_resultados[
        df_resultados["DATA_ATTEMPTS"]
        > 1
    ]

    # --------------------------------------------------------
    # Recuperados
    # --------------------------------------------------------

    recuperados = df_resultados[
        (
            df_resultados["DATA_SOURCE"]
            == "INDIVIDUAL_RETRY"
        )
        &
        (
            df_resultados["STATUS"]
            == "OK"
        )
    ]

    # --------------------------------------------------------
    # Delisted
    # --------------------------------------------------------

    delisted = df_resultados[
        df_resultados["STATUS"]
        == "DELISTED"
    ]

    # --------------------------------------------------------
    # Inactive / unavailable
    # --------------------------------------------------------

    inactive = df_resultados[
        df_resultados["STATUS"]
        == "INACTIVE_OR_UNAVAILABLE"
    ]

    # --------------------------------------------------------
    # Insufficient
    # --------------------------------------------------------

    insufficient = df_resultados[
        df_resultados["STATUS"]
        == "INSUFFICIENT_DATA"
    ]

    # --------------------------------------------------------
    # Error
    # --------------------------------------------------------

    errores = df_resultados[
        df_resultados["STATUS"]
        == "ERROR"
    ]

    print(
        f"Tickers que necesitaron retry : "
        f"{len(retries)}"
    )

    print(
        f"Recuperados mediante retry    : "
        f"{len(recuperados)}"
    )

    print(
        f"DELISTED                      : "
        f"{len(delisted)}"
    )

    print(
        f"INACTIVE / UNAVAILABLE        : "
        f"{len(inactive)}"
    )

    print(
        f"INSUFFICIENT_DATA             : "
        f"{len(insufficient)}"
    )

    print(
        f"ERROR                         : "
        f"{len(errores)}"
    )

    # --------------------------------------------------------
    # Detalle de retries
    # --------------------------------------------------------

    if not retries.empty:

        print()
        print(
            "📋 TICKERS QUE NECESITARON RETRY:"
        )

        for _, fila in retries.iterrows():

            print(
                f"  • {fila['Ticker']} | "
                f"{fila['STATUS']} | "
                f"{fila['DATA_ATTEMPTS']} intentos | "
                f"{fila.get('DATA_OBSERVATIONS', 0)} obs"
            )

    # --------------------------------------------------------
    # Delisted
    # --------------------------------------------------------

    if not delisted.empty:

        print()
        print(
            "⛔ TICKERS CLASIFICADOS COMO DELISTED:"
        )

        for _, fila in delisted.iterrows():

            print(
                f"  ❌ {fila['Ticker']} | "
                f"{fila.get('DATA_OBSERVATIONS', 0)} obs | "
                f"{fila.get('DATA_LAST_DATE', '')}"
            )

    # --------------------------------------------------------
    # Otros problemas
    # --------------------------------------------------------

    otros_problemas = df_resultados[
        ~df_resultados["STATUS"].isin(
            [
                "OK",
                "DELISTED"
            ]
        )
    ]

    if not otros_problemas.empty:

        print()
        print(
            "⚠️ OTROS TICKERS CON PROBLEMAS:"
        )

        for _, fila in otros_problemas.iterrows():

            print(
                f"  • {fila['Ticker']} | "
                f"{fila['STATUS']} | "
                f"{fila.get('ERROR', '')}"
            )


# ============================================================
# 15. CONSTRUIR CSV
# ============================================================

columnas = [

    "RUN_ID",

    "CAPTURED_AT_UTC",

    "Ticker",

    "STATUS",

    "ERROR",

    "DATA_ATTEMPTS",

    "DATA_SOURCE",

    "DATA_OBSERVATIONS",

    "DATA_FIRST_DATE",

    "DATA_LAST_DATE",

    "DATA_DIAGNOSIS",

    "Price_Actual",

    "Price_Close",

    "R1",

    "R5",

    "R20",

    "R60",

    "R120",

    "Vol5",

    "Vol20",

    "Vol60",

    "Vol252",

    "VolRatio20",

    "VolRatio60",

    "Skew20",

    "Kurt20",

    "AC1",

    "Forward5",

    "Forward10",

    "Forward20",

    "Forward40",

    "EV5",

    "EV10",

    "EV20",

    "EV40",

    "Confidence5",

    "Confidence10",

    "Confidence20",

    "Confidence40",

    "ESS5",

    "ESS10",

    "ESS20",

    "ESS40",

    "ES95_5",

    "ES95_10",

    "ES95_20",

    "ES95_40",

    "Kelly25"
]


# ------------------------------------------------------------
# Crear columnas faltantes
# ------------------------------------------------------------

for columna in columnas:

    if columna not in df_resultados.columns:

        df_resultados[
            columna
        ] = np.nan


df_export = df_resultados[
    columnas
].copy()


# ------------------------------------------------------------
# Limpieza de NaN / Inf
# ------------------------------------------------------------

df_export = df_export.replace(
    [np.inf, -np.inf],
    np.nan
)

df_export = df_export.fillna("")


# ------------------------------------------------------------
# CSV
# ------------------------------------------------------------

texto_csv = df_export.to_csv(

    index=False,

    lineterminator="\n"
)


# ============================================================
# 16. EXPORTAR A GOOGLE SHEETS
# ============================================================

print()

print(
    "📤 Exportando CERE a Google Sheets..."
)

try:

    request = urllib.request.Request(

        URL_RECEPTORA_GOOGLE,

        data=texto_csv.encode(
            "utf-8"
        ),

        headers={

            "User-Agent":
                "CERE-GitHub/1.1",

            "Content-Type":
                "text/csv; charset=utf-8"
        },

        method="POST"
    )

    with urllib.request.urlopen(

        request,

        timeout=60

    ) as response:

        respuesta = (

            response

            .read()

            .decode(
                "utf-8"
            )
        )

    print()

    print(
        "🎉 GOOGLE SHEETS RESPONDIÓ:"
    )

    print(
        respuesta
    )

except Exception as e:

    print()

    print(
        "❌ ERROR AL EXPORTAR "
        "A GOOGLE SHEETS:"
    )

    print(
        f"{type(e).__name__}: "
        f"{str(e)}"
    )

    raise


# ============================================================
# 17. RESUMEN FINAL
# ============================================================

print()

print("=" * 70)

print(
    "✅ CERE v1.1 TERMINADO"
)

print("=" * 70)

print(
    f"RUN_ID          : "
    f"{run_id}"
)

print(
    f"Universo        : "
    f"{total_universo}"
)

print(
    f"Procesados      : "
    f"{total_procesados}"
)

print(
    f"Estado OK       : "
    f"{total_ok}"
)

print(
    f"Con problemas   : "
    f"{total_error}"
)

print(
    f"  DELISTED      : "
    f"{total_delisted}"
)

print(
    f"  INACTIVE      : "
    f"{total_inactive}"
)

print(
    f"  INSUFFICIENT  : "
    f"{total_insufficient}"
)

print(
    f"  ERROR         : "
    f"{total_errors}"
)

print(
    f"Tiempo cálculo  : "
    f"{tiempo_calculo:.1f} s"
)

print(
    "Google Sheets   : EXPORTADO"
)

print("=" * 70)
