import numpy as np
import pandas as pd
import yfinance as yf
from scipy.stats import t
from concurrent.futures import ThreadPoolExecutor
import urllib.request
import urllib.parse

print("🌐 1. Cargando Universo Cuantitativo Optimizado Personalizado...")

universo_tickers = [
    "AAPL", "NVDA", "AMD", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "INTC", "QCOM", "AVGO", "NFLX", "CSCO", "AMAT", "MU",
    "PANW", "SNPS", "CDNS", "PLTR", "PYPL", "SHOP", "NET", "DDOG", "CRWD", "OKTA", "ZS", "MDB", "TEAM", "WDAY",
    "NOW", "SNOW", "ZM", "DOCU", "ROKU", "TWLO", "PINS", "SNAP", "MTCH", "TSM", "ASML", "LRCX", "KLAC", "NXPI", "TXN", 
    "ADI", "MCHP", "ON", "MRVL", "TER", "ENPH", "SEDG", "FSLR", "FLEX", "COIN", "MARA", "RIOT", "SOFI", "AFRM", "UPST", 
    "HOOD", "DKNG", "NU", "MELI", "SE", "V", "MA", "AXP", "REGN", "BIIB", "GILD", "AMGN", "VRTX", "ILMN", "ALGN", 
    "MRNA", "BNTX", "CRSP", "EDIT", "NTLA", "BEAM", "SBUX", "MDLZ", "CHTR", "TMUS", "CMCSA", "EA", "TTWO", "ABNB", 
    "BKNG", "EXPE", "TRIP", "PDD", "JD", "BABA", "BIDU", "NIO", "LI", "XPEV", "LCID", "RIVN", "QS", "PLUG", 
    "RUN", "CHPT", "BLNK", "BE", "FCEL", "SPWR", "CAT", "DE", "HON", "GE", "MMM", "LMT", "BA", "NOC", 
    "GD", "RTX", "UPS", "FDX", "CSX", "NSC", "UNP", "WM", "RSG", "JPM", "BAC", "WFC", "C", "GS", 
    "MS", "BLK", "BX", "KKR", "APO", "TROW", "BEN", "STT", "NTRS", "SCHW", "AMTD", "WMT", "TGT", 
    "COST", "HD", "LOW", "PG", "KO", "PEP", "EL", "CL", "KMB", "GIS", "MNST", "CELH", "XOM", 
    "CVX", "COP", "EOG", "SLB", "HAL", "BKR", "OXY", "DVN", "APA", "FCX", "NEM", "NUE", 
    "STLD", "AA", "CLF", "T", "VZ", "DIS", "WBD", "PARA", "FOXA", "NWSA", "LYV", "SIFY", 
    "VOD", "TELFY", "LIN", "APD", "ECL", "SHW", "DD", "DOW", "MOS", "CF", "NTR", "FMC", 
    "VMC", "MLM", "CX", "LEN", "DHI", "PHM", "PLD", "AMT", "CCI", "EQIX", "O", "SPG", 
    "PSA", "EXR", "AVB", "EQR", "MAA", "VICI", "DLR", "SBAC", "WY", "BXP", "ISRG", "SYK", 
    "ZBH", "EW", "BSX", "MDT", "ABT", "BMY", "PFE", "JNJ", "LLY", "NVO", "AZN", "SNY", 
    "GSK", "TAK", "Z", "ZG", "OPEN", "COMP", "W", "CVNA", "CHWY", "JMIA", "EBAY", "ETSY", 
    "WAY", "PAGS", "STNE", "PAYS", "FLYW", "LOT", "LPRO", "OPFI", "CACC", "OMF", "FCF", 
    "GPRO", "FITB", "HBAN", "KEY", "RF", "CFG", "MTB", "ZION", "TFC", "AX", "CUBI", "HOMB", 
    "OZK", "FNB", "ASB", "VLY", "UMBF", "BOKF", "EGBN", "WBS", "CATY", "IWM", "QQQ", 
    "VEA", "VWO", "IEFA", "EEM", "VNQ", "GLD", "USO", "UNG", "OIH", "XLE", "XLF", "XLK", 
    "XLV", "XLY", "XLP", "XLI", "XLB", "XLU", "XLRE", "SMH", "SOXX", "XBI", "KRE", "JETS", 
    "ARKK", "ARKW", "ARKG", "ARKF", "ARKQ", "BITO", "RUM", "DJT", "PSNY", "MP", "SMCI", 
    "DELL", "ANET", "VRT", "LITE", "CLS", "MOD", "CRDO", "ALAB", "GLW", "CEG", "VST", 
    "GEV", "ETN", "PWR", "APP", "AXON", "RDDT", "DUOL", "CAVA", "TEM", "TMDX", "IREN", 
    "RKLB", "ASTS", "HUT", "WULF", "CIFR", "ATI", "XYZ", "HAPN"
]

universo_tickers = list(sorted(set(universo_tickers)))
print(f"📥 2. Descargando precios históricos para {len(universo_tickers)} activos vigentes...")
data_descarga = yf.download(universo_tickers, period="2y", auto_adjust=True, progress=False)

def analizar_datos_ticker(ticker):
    try:
        if isinstance(data_descarga.columns, pd.MultiIndex):
            if ticker not in data_descarga['Close'].columns: return None
            datos = data_descarga['Close'][ticker].dropna()
        else:
            if ticker not in data_descarga.columns: return None
            datos = data_descarga['Close'].dropna()
            
        if datos.empty or len(datos) < 50: return None
        precios = datos.values.flatten()
        retornos_log = np.log(precios[1:] / precios[:-1])
        df_t, loc_t, scale_t = t.fit(retornos_log)
        p_real = 1 - t.cdf(0, df_t, loc=loc_t, scale=scale_t)
        volatilidad_anual = retornos_log.std() * np.sqrt(252)
        if volatilidad_anual <= 0: volatilidad_anual = 0.35
        q_mercado = 0.50 
        b_cuota = (1 - q_mercado) / q_mercado + (volatilidad_anual * 4)
        ev = (p_real * b_cuota) - (1 - p_real)
        
        if ev > 0.0: 
            return f"{ticker},{round(float(precios[-1]), 2)},{round(float(p_real), 2)},{round(float(b_cuota), 2)},{round(float(ev), 2)}"
    except: return None

resultados = ["Ticker,Precio_Actual,Probabilidad_Real_P,Cuota_Market_B,Valor_Esperado_EV"]
print("⚙️ 3. Ejecutando escáner matemático paralelo...")
with ThreadPoolExecutor(max_workers=40) as executor:
    for res in executor.map(analizar_datos_ticker, universo_tickers):
        if res is not None: resultados.append(res)

texto_final = "\n".join(resultados)

# 🚀 EXPORTACIÓN COMPORTAMIENTO NATIVO (SUBIR A DPASTE COMO TEXTO PLANO)
try:
    data_post = urllib.parse.urlencode({'content': texto_final, 'expiry_days': 1}).encode('utf-8')
    req = urllib.request.Request("https://dpaste.org", data=data_post)
    with urllib.request.urlopen(req) as response:
        url_resultado = response.read().decode('utf-8').strip()
    
    # Imprimimos el enlace exacto con el sufijo /raw para que Google Sheets lo chupe sin formato HTML
    url_raw = url_resultado + "/raw"
    print("\n=======================================================")
    print(f"ENLACE_AUTOMATICO_GENERADO: {url_raw}")
    print("=======================================================")
except Exception as e:
    print(f"\n⚠️ Error al exportar: {e}")
