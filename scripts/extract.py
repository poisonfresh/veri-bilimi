# Dosya: C:\termproject_datasci\scripts\extract.py
import yfinance as yf
import pandas as pd

def extract_data(stock_list, index_list):
    print("--- Yahoo Finance'ten Veri Çekiliyor ---")
    all_tickers = stock_list + index_list
    
    # Son 10 yıllık veriyi indiriyoruz
    raw_data = yf.download(all_tickers, start="2014-01-01", end="2026-01-01", group_by='ticker')
    
    processed_data_list = []
    
    for ticker in all_tickers:
        try:
            df = raw_data[ticker].copy()
            if df.empty:
                print(f"Uyarı: {ticker} verisi boş geldi.")
                continue
            
            df = df.reset_index()
            df['ticker'] = ticker
            # Sütun isimlerini düzelt (Open -> open)
            df.columns = [c.lower().replace(' ', '_') for c in df.columns]
            
            processed_data_list.append(df)
            print(f"✅ {ticker} indirildi.")
            
        except Exception as e:
            print(f"❌ Hata ({ticker}): {e}")

    return processed_data_list