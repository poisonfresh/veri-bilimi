from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    hour, col, corr, count, sum as _sum, avg, desc, max as _max, row_number
)
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
import pyspark.sql.functions as F

# Spark Yapılandırması
conf = SparkConf().setAppName("Homework2").setMaster("local[*]")
spark = SparkSession.builder.config(conf=conf).getOrCreate()
sc = spark.sparkContext

# Zone lookup tablosunu oku (Global olarak veya main içinde okunabilir, burada global kolaylık sağlar)
zones_df = spark.read.csv("taxi_zone_lookup.csv", header=True, inferSchema=True)


def find_statistical_information(df):

    print("--- Statistical Information ---")
    df.select(
        count("*").alias("Total_Trips"),
        avg("total_amount").alias("Avg_Total_Amount"),
        avg("trip_distance").alias("Avg_Trip_Distance"),
        _max("trip_distance").alias("Max_Trip_Distance")
    ).show()


def join_look_up_with_cities(df):

    df = df.join(
        zones_df.withColumnRenamed("LocationID", "PULocationID")
        .withColumnRenamed("Borough", "Pickup_Borough")
        .withColumnRenamed("Zone", "Pickup_Zone"),
        on="PULocationID", how="left"
    )
    # Dropoff Zone Join
    df = df.join(
        zones_df.withColumnRenamed("LocationID", "DOLocationID")
        .withColumnRenamed("Borough", "Dropoff_Borough")
        .withColumnRenamed("Zone", "Dropoff_Zone"),
        on="DOLocationID", how="left"
    )
    return df


def get_most_expensive_route(df):

    """En pahalı rota, ortalama ücretin (total_amount) en yüksek olduğu Pickup-Dropoff çiftidir.Gürültüyü önlemek için en az 5 sefer yapılmış rotaları baz alıyoruz."""
    print("--- Most Expensive Route (Avg Amount) ---")
    df.groupBy("Pickup_Zone", "Dropoff_Zone") \
        .agg(avg("total_amount").alias("avg_cost"), count("*").alias("trip_count")) \
        .filter(col("trip_count") > 5) \
        .orderBy(desc("avg_cost")) \
        .show(1)


def get_busiest_taxi_station(df):
    """Genellikle en çok yolcu alınan (Pickup) yer kastedilir."""
    print("--- Busiest Taxi Station ---")
    df.groupBy("Pickup_Zone") \
        .count() \
        .orderBy(desc("count")) \
        .show(1)


def get_top_5_busiest_area(df):

    print("--- Top 5 Busiest Pickup Areas ---")
    df.groupBy("Pickup_Zone") \
        .count() \
        .orderBy(desc("count")) \
        .show(5)


def get_longest_trips(df):

    print("--- Longest Trip ---")
    df.orderBy(desc("trip_distance")) \
        .select("Pickup_Zone", "Dropoff_Zone", "trip_distance", "total_amount") \
        .show(1)


def get_crowded_places_per_hour(df):
    """ Her saat dilimi için en kalabalık Pickup ve Dropoff bölgesini bulur. Window fonksiyonu kullanarak her saatin 'rank 1' olan bölgesini seçer."""
    # Saat kolonunu oluştur (Pickup ve Dropoff zamanına göre ayrı ayrı bakılabilir, genelde pickup baz alınır ama soru ikisini de istiyor)
    # Not: Veri setine göre kolon isimleri değişebilir (tpep_pickup_datetime vs lpep_pickup_datetime).
    # Bu fonksiyonu çağırırken kolon ismi dinamik olmadığı için kod içinde kontrol ediyoruz.

    if "tpep_pickup_datetime" in df.columns:
        time_col_pu = "tpep_pickup_datetime"
        time_col_do = "tpep_dropoff_datetime"
    else:
        time_col_pu = "lpep_pickup_datetime"
        time_col_do = "lpep_dropoff_datetime"

    print("--- Crowded Pickup Zones Per Hour ---")
    df_pu = df.withColumn("hour", hour(col(time_col_pu)))
    w = Window.partitionBy("hour").orderBy(desc("count"))

    df_pu.groupBy("hour", "Pickup_Zone").count() \
        .withColumn("rank", row_number().over(w)) \
        .filter(col("rank") == 1) \
        .orderBy("hour") \
        .show(24)

    print("--- Crowded Drop-off Zones Per Hour ---")
    df_do = df.withColumn("hour", hour(col(time_col_do)))

    df_do.groupBy("hour", "Dropoff_Zone").count() \
        .withColumn("rank", row_number().over(w)) \
        .filter(col("rank") == 1) \
        .orderBy("hour") \
        .show(24)


def get_busiest_hours(df):
    """Returns a single dataframe suitable for plotting."""
    if "tpep_pickup_datetime" in df.columns:
        pu_col = "tpep_pickup_datetime"
        do_col = "tpep_dropoff_datetime"
    else:
        pu_col = "lpep_pickup_datetime"
        do_col = "lpep_dropoff_datetime"

    # Pickup counts per hour
    pu_counts = df.withColumn("hour", hour(col(pu_col))) \
        .groupBy("hour").count().withColumnRenamed("count", "pickup_count")

    # Dropoff counts per hour
    do_counts = df.withColumn("hour", hour(col(do_col))) \
        .groupBy("hour").count().withColumnRenamed("count", "dropoff_count")

    # Join them
    stats_df = pu_counts.join(do_counts, on="hour", how="outer").orderBy("hour").fillna(0)

    return stats_df


def draw_busiest_hours_graph(df, dataset_name):
    # Spark DF -> Pandas DF
    pdf = df.toPandas()

    plt.figure(figsize=(10, 6))
    plt.plot(pdf['hour'], pdf['pickup_count'], marker='o', label='Pickup Count')
    plt.plot(pdf['hour'], pdf['dropoff_count'], marker='x', label='Dropoff Count')

    # Başlığa dataset ismini ekliyoruz
    plt.title(f"{dataset_name} - Hourly Pickup and Drop-off Counts")
    plt.xlabel("Hour of Day")
    plt.ylabel("Number of Trips")
    plt.xticks(range(0, 24))
    plt.grid(True)
    plt.legend()

    # Dosya ismine de dataset ismini ekliyoruz
    filename = f"hourly_counts_{dataset_name.replace(' ', '_')}.png"
    plt.savefig(filename)
    plt.show()


def get_tip_correlation(df):

    print("--- Tip Correlation Analysis ---")
    for col_name in ["trip_distance", "fare_amount", "total_amount", "tolls_amount"]:
        val = df.select(corr("tip_amount", col_name)).collect()[0][0]
        print(f"Correlation between tip_amount and {col_name}: {val}")


if __name__ == '__main__':

    try:
        yellow_df = spark.read.parquet("yellow_tripdata_2021-03.parquet")
        green_df = spark.read.parquet("green_tripdata_2021-03.parquet")
    except Exception as e:
        print("Dosyalar bulunamadı, lütfen dosya yollarını kontrol edin.")
        # Hata durumunda boş DF ile devam etmemek için çıkış yapılabilir veya mock data kullanılabilir.
        # Burada örnek devamlılık için exit kullanmıyorum ama gerçek senaryoda dosya şart.
        yellow_df = None
        green_df = None

    if yellow_df:
        print("===== YELLOW TAXI ANALYSIS =====")
        # 1. Join Lookup
        yellow_df = join_look_up_with_cities(yellow_df)

        # 2. Warmup Stats
        find_statistical_information(yellow_df)

        # 3. Questions
        get_most_expensive_route(yellow_df)
        get_busiest_taxi_station(yellow_df)
        get_top_5_busiest_area(yellow_df)
        get_longest_trips(yellow_df)

        # 4. Hourly Analysis
        get_crowded_places_per_hour(yellow_df)
        hourly_stats_yellow = get_busiest_hours(yellow_df)
        draw_busiest_hours_graph(hourly_stats_yellow, "Yellow Taxi")

        # 5. Bonus
        get_tip_correlation(yellow_df)

    if green_df:
        print("\n===== GREEN TAXI ANALYSIS =====")
        green_df = join_look_up_with_cities(green_df)

        find_statistical_information(green_df)
        get_most_expensive_route(green_df)
        get_busiest_taxi_station(green_df)
        get_top_5_busiest_area(green_df)
        get_longest_trips(green_df)

        get_crowded_places_per_hour(green_df)
        hourly_stats_green = get_busiest_hours(green_df)
        draw_busiest_hours_graph(hourly_stats_green, "Green Taxi")

        get_tip_correlation(green_df)

    spark.stop()