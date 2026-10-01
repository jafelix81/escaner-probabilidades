import numpy as np
import pandas as pd
import yfinance as yf
from scipy.stats import t
from concurrent.futures import ThreadPoolExecutor

print("🌐 1. Cargando Universo Cuantitativo Optimizado...")

# Universo de acciones saneado y libre de errores de red
universo_tickers = [
    "AAPL", "NVDA", "AMD", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "INTC", "QCOM", "AVGO", "NFLX", "CSCO", "AMAT", "MU",
    "PANW", "SNPS", "CDNS", "PLTR", "SQ", "PYPL", "SHOP", "NET", "DDOG", "CRWD", "OKTA", "ZS", "MDB", "TEAM", "WDAY",
    "NOW", "SNOW", "ZM", "DOCU", "ROKU", "TWLO", "PINS", "SNAP", "MTCH", "SPLK", "FIVN", "RING", "PD", "DT", "NEWR",
    "TSM", "ASML", "LRCX", "KLAC", "NXPI", "TXN", "ADI", "MCHP", "ON", "MRVL", "TER", "ENPH", "SEDG", "FSLR", "FLEX",
    "COIN", "MARA", "RIOT", "SOFI", "AFRM", "UPST", "HOOD", "DKNG", "NU", "MELI", "SE", "V", "MA", "AXP", "DFS",
    "REGN", "BIIB", "GILD", "AMGN", "VRTX", "ILMN", "ALGN", "MRNA", "BNTX", "SGEN", "CRSP", "EDIT", "NTLA", "BEAM",
    "SBUX", "MDLZ", "CHTR", "TMUS", "CMCSA", "EA", "TTWO", "ABNB", "BKNG", "EXPE", "TRIP", "PDD", "JD", "BABA", "BIDU",
    "NIO", "LI", "XPEV", "LCID", "RIVN", "QS", "NKLA", "PLUG", "RUN", "CHPT", "BLNK", "BE", "FCEL", "SPWR", "NOVA",
    "CAT", "DE", "HON", "GE", "MMM", "LMT", "BA", "NOC", "GD", "RTX", "UPS", "FDX", "CSX", "NSC", "UNP", "WM", "RSG",
    "JPM", "BAC", "WFC", "C", "GS", "MS", "BLK", "BX", "KKR", "APO", "TROW", "BEN", "STT", "NTRS", "SCHW", "AMTD",
    "WMT", "TGT", "COST", "HD", "LOW", "PG", "KO", "PEP", "EL", "CL", "KMB", "GIS", "MNST", "CELH",
    "XOM", "CVX", "COP", "EOG", "SLB", "HAL", "BKR", "OXY", "DVN", "APA", "FCX", "NEM", "NUE", "STLD", "AA", "CLF",
    "T", "VZ", "DIS", "WBD", "PARA", "FOXA", "NWSA", "LYV", "SIFY", "VOD", "ORAN", "TEF",
    "LIN", "APD", "ECL", "SHW", "DD", "DOW", "MOS", "CF", "NTR", "FMC", "VMC", "MLM", "CX", "LEN", "DHI", "PHM",
    "PLD", "AMT", "CCI", "EQIX", "O", "SPG", "PSA", "EXR", "AVB", "EQR", "MAA", "VICI", "DLR", "SBAC", "WY", "BXP",
    "ISRG", "SYK", "ZBH", "EW", "BSX", "MDT", "ABT", "BMY", "PFE", "JNJ", "LLY", "NVO", "AZN", "SNY", "GSK", "TAK",
    "Z", "ZG", "OPEN", "RDFN", "COMP", "EXPI", "W", "CVNA", "CHWY", "JMIA", "EBAY", "ETSY", "WAY", "OZON", "PAGS",
    "STONE", "STNE", "PPRO", "PAYS", "FLYW", "SHIFT", "LOT", "LC", "LPRO", "OPFI", "CACC", "OMF", "SC", "FCF",
    "GPRO", "FITB", "HBAN", "KEY", "RF", "CFG", "MTB", "CMA", "ZION", "TFC", "NYCB", "AX", "CUBI", "HOMB", "OZK",
    "FNB", "ASB", "VLY", "UMBF", "BOKF", "EGBN", "WBS", "CATY", "IWM", "QQQ", "DIA", "VOO", "IVV", "VTI", "VEA",
    "VWO", "IEFA", "EEM", "AGG", "BND", "LQD", "HYG", "TIP", "VNQ", "GLD", "SLV", "USO", "UNG", "DBA", "UUP",
    "FXE", "FXY", "TLT", "IEF", "SHY", "BIL", "MINT", "NEAR", "PULS", "ICSH", "FLOT", "OIH", "XLE", "XLF",
    "XLK", "XLV", "XLY", "XLP", "XLI", "XLB", "XLU", "XLRE", "SMH", "SOXX", "XBI", "KRE", "KW", "JETS",
    "NERD", "HERO", "GAMR", "MJ", "MSOS", "YOLO", "CNBS", "THCX", "POTX", "HMMJ", "BLOK", "LEGR", "BLCN",
    "ARKK", "ARKW", "ARKG", "ARKF", "ARKQ", "PRNT", "IZRL", "BITO", "GBTC", "ETHE", "OBTC", "LTCN", "BCHG", "GDLC",
    "PSTH", "IPOD", "IPOF", "IPOX", "SPAK", "SPCX", "ALCC", "DWAC", "CFVI", "GGPI", "BSGA", "APAC", "MCAA", "PNTM",
    "RUM", "SUB", "MUB", "HYMB", "NYF", "TFI", "CMF", "PZA", "ITM", "SHM", "VRDO", "FMB", "JMUB", "SEM", "MUNI",
    "BNDX", "VWOB", "EMB", "VWRL", "VUSA", "VUKE", "VHYL", "VMID", "VGOV", "VECP", "VERX", "VETY", "VAPX", "VFEM",
    "IQDF", "DEFA", "EFAV", "HEFA", "IEMG", "SCZ", "IEUR", "IPAC", "AAXJ", "MCHI", "EWJ", "EWG", "EWU", "EWC", "EWA",
    "EWT", "EWY", "EWM", "EWS", "EWH", "EWP", "EWI", "EWN", "EWD", "EWL", "EWQ", "EWO", "EWR", "EWZ", "EWW", "ECH"
]

universo_tickers = list(sorted(set(universo_tickers)))
print(f"📥 2. Descargando precios históricos para {len(universo_tickers)} activos seleccionados...")
data_descarga = yf.download(universo_tickers, period="2y", auto_adjust=True, progress=False)

def analizar_datos_ticker(ticker):
    try:
        if isinstance(data_descarga.columns, pd.MultiIndex):
            datos = data_descarga['Close'][ticker].dropna()
        else:
            datos = data_descarga['Close'].dropna()
            
        if datos.empty or len(datos) < 50: return None
        
        precios = datos.values.flatten()
        retornos_log = np.log(precios[1:] / precios[:-1])
        
        # 1. Ajuste de Colas Pesadas (Mandelbrot)
        df_t, loc_t, scale_t = t.fit(retornos_log)
        p_real = 1 - t.cdf(0, df_t, loc=loc_t, scale=scale_t)
        
        # 2. Volatilidad Histórica Externa
        volatilidad_anual = retornos_log.std() * np.sqrt(252)
        if volatilidad_anual <= 0: volatilidad_anual = 0.35
        
        q_mercado = 0.50 
        b_cuota = (1 - q_mercado) / q_mercado + (volatilidad_anual * 4)
        
        # 3. Métrica de Valor Esperado (EV)
        ev = (p_real * b_cuota) - (1 - p_real)
        
        if ev > 0.0: 
            return {
                "Ticker": ticker, 
                "Precio_Actual": round(float(precios[-1]), 2), 
                "Probabilidad_Real_P": round(float(p_real), 2), 
                "Cuota_Market_B": round(float(b_cuota), 2), 
                "Valor_Esperado_EV": round(float(ev), 2)
            }
    except:
        return None

resultados = []
print("⚙️ 3. Ejecutando escáner matemático paralelo...")
with ThreadPoolExecutor(max_workers=40) as executor:
    for res in executor.map(analizar_datos_ticker, universo_tickers):
        if res is not None: 
            resultados.append(res)

df_final = pd.DataFrame(resultados)

print("\n=======================================================")
print("📬 MATRIZ DE PROBABILIDADES ACTUALIZADA:")
print("=======================================================")
if not df_final.empty:
    df_final = df_final.sort_values(by="Valor_Esperado_EV", ascending=False)
    print(df_final.to_string(index=False))
else:
    print("Hoy el mercado se encuentra en perfecto equilibrio matemático.")
print("=======================================================")
