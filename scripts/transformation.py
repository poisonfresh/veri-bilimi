import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.sql.functions import to_date, col
import numpy as np

def transform_data(spark, data_list):
    print("--- Veri Dönüştürme (Transformation) Başladı ---")
    
    if not data_list:
        print("❌ Dönüştürülecek veri yok!")
        return None

    # 1. Pandas DataFrame'i Birleştir
    full_pdf = pd.concat(data_list)
    
    # 2. Sütun İsimlerini Temizle
    full_pdf.columns = [c.lower().replace(' ', '_') for c in full_pdf.columns]
    
    # 3. Eksik Sütun Kontrolleri
    if 'adj_close' not in full_pdf.columns:
        full_pdf['adj_close'] = full_pdf['close']

    if 'volume' not in full_pdf.columns:
        full_pdf['volume'] = 0

    # 4. Sütun Sıralaması ve Temizlik
    required_columns = ['date', 'open', 'high', 'low', 'close', 'adj_close', 'volume', 'ticker']
    full_pdf = full_pdf[required_columns]
    
    # Boş değerleri (NaN) 0 ile doldur (Çok Önemli!)
    full_pdf = full_pdf.fillna(0)
    
    # --- KRİTİK DÜZELTME BURADA ---
    # Volume sütununu ondalıklı sayıdan (2340.0) tam sayıya (2340) çeviriyoruz.
    # Spark LongType için int64 şarttır.
    full_pdf['volume'] = full_pdf['volume'].astype('int64')
    
    # Tarihi string yap
    full_pdf['date'] = full_pdf['date'].astype(str)

    # 5. Spark Şeması
    schema = StructType([
        StructField("date", StringType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("adj_close", DoubleType(), True),
        StructField("volume", LongType(), True),
        StructField("ticker", StringType(), True)
    ])
    
    print(f"ℹ️ Pandas DataFrame Boyutu: {full_pdf.shape}")
    
    # 6. Spark DataFrame Oluşturma
    spark_df = spark.createDataFrame(full_pdf, schema=schema)
    
    # Veritabanı sütun isimlerine (price ekleri) çevir
    spark_df = spark_df.withColumn("date", to_date(col("date"), "yyyy-MM-dd")) \
                       .withColumnRenamed("open", "open_price") \
                       .withColumnRenamed("high", "high_price") \
                       .withColumnRenamed("low", "low_price") \
                       .withColumnRenamed("close", "close_price")
                       
    print("✅ Spark DataFrame başarıyla oluşturuldu.")
    return spark_df