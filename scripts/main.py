import sys
import os
import glob

# --- KRİTİK WINDOWS AYARLARI (En Başta Olmalı) ---
# Spark ve Hadoop'un yerini Python'a sert bir şekilde öğretiyoruz.
spark_location = "C:\\Spark"

os.environ['SPARK_HOME'] = spark_location
os.environ['HADOOP_HOME'] = spark_location

# Windows'un winutils.exe'yi bulabilmesi için "bin" klasörünü sistem yoluna ekliyoruz
sys.path.append(spark_location + "\\bin")
os.environ['PATH'] += os.pathsep + spark_location + "\\bin"
os.environ['PATH'] += os.pathsep + spark_location

# Python Yolları (Sanal Ortamını Gösteriyoruz)
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# --- FINSPARK BAŞLATMA ---
import findspark
findspark.init(spark_location)

# --- SPARK MODÜLLERİ ---
from pyspark.sql import SparkSession

# Kendi modüllerimiz
from extract import extract_data
from transformation import transform_data
from load import load_data

# --- DRIVER DOSYASI KONTROLÜ ---
driver_folder = "C:\\termproject_datasci\\drivers"
jar_files = glob.glob(os.path.join(driver_folder, "postgresql*.jar"))

if not jar_files:
    print(f"❌ HATA: {driver_folder} içinde .jar dosyası yok!")
    sys.exit(1)

driver_path = jar_files[0]
print(f"ℹ️ Kullanılan Driver: {driver_path}")

def main():
    print("🚀 Spark Oturumu Başlatılıyor (Hadoop Ayarları Yüklendi)...")
    
    # Spark Session Oluşturma
    spark = SparkSession.builder \
        .appName("YahooFinanceETL") \
        .config("spark.driver.extraClassPath", driver_path) \
        .config("spark.jars", driver_path) \
        .config("spark.sql.warehouse.dir", "file:///C:/temp") \
        .config("spark.hadoop.home.dir", spark_location) \
        .getOrCreate()

    # 1. Tanımlar
    print("📋 Hisseler tanımlanıyor...")
    stock_list = ['AAPL', 'NVDA', 'MSFT', 'AVGO', 'META', 'AMZN', 'TSLA']
    index_list = ['^GSPC', 'NQ=F', 'RTY=F', '^DJI']

    # 2. Extract
    raw_data_list = extract_data(stock_list, index_list)

    # 3. Transform
    final_df = transform_data(spark, raw_data_list)
    
    if final_df is None:
        print("⚠️ İşlem durduruldu: Veri seti boş.")
        spark.stop()
        return

    # 4. Load
    print("💾 Veritabanı ayarları yapılıyor...")
    db_config = {
        "host": "localhost",
        "port": "5432",
        "user": "postgres",
        "password": "12345",  
        "dbname": "term_project_db"
    }

    load_data(final_df, "stock_prices", db_config)

    print("🏁 TEBRİKLER! ETL Süreci Başarıyla Tamamlandı.")
    spark.stop()

if __name__ == '__main__':
    main()