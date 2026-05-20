# Dosya: C:\termproject_datasci\scripts\load.py

def load_data(spark_df, table_name, db_config):
    print(f"--- {table_name} Tablosuna Yazılıyor ---")
    
    jdbc_url = f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
    
    try:
        spark_df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", db_config['user']) \
            .option("password", db_config['password']) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
            
        print(f"🎉 Başarılı: Veriler veritabanına yüklendi!")
    except Exception as e:
        print(f"❌ Veritabanı Hatası: {e}")