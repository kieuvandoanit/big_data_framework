#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as F_sum, substring, input_file_name,
    regexp_extract, lit
)
import sys


def build_spark_session():
    spark = (
        SparkSession.builder
        .appName("OrdersAnalytics")
        # nếu chạy trong spark-master bde2020, master có thể là local hoặc spark://spark-master:7077
        # .master("spark://spark-master:7077")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_orders_df(spark):
    # Đọc tất cả file CSV trong /data trên HDFS
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("hdfs://namenode:8020/data/*.csv")
    )

    # Chuẩn hoá kiểu dữ liệu
    df = df.select(
        col("OrderID").cast("string"),
        col("ProductID").cast("int"),
        col("ProductName").cast("string"),
        col("Amount").cast("int"),
        col("Price").cast("double"),
        col("Discount").cast("double")
    )

    # Thêm cột năm-tháng từ OrderID: 2021010100001000 -> 202101, 2021
    df = (
        df
        .withColumn("order_ym", substring("OrderID", 1, 6))   # YYYYMM
        .withColumn("order_year", substring("OrderID", 1, 4)) # YYYY
    )

    # Lấy ShopID từ tên file: .../Shop-6-20210101-00.csv -> 6
    df = df.withColumn("input_file", input_file_name())
    df = df.withColumn(
        "ShopID",
        regexp_extract(col("input_file"), r"Shop-(\d+)-", 1)
    )

    # Doanh thu = Amount * Price * (1 - Discount/100)
    df = df.withColumn(
        "Revenue",
        col("Amount") * col("Price") * (1 - col("Discount") / lit(100.0))
    )

    return df


# a) Top K sản phẩm bán chạy nhất toàn hệ thống
def top_k_products(df, k):
    k = int(k)
    result = (
        df.groupBy("ProductID", "ProductName")
          .agg(F_sum("Amount").alias("TotalAmount"))
          .orderBy(col("TotalAmount").desc())
          .limit(k)
    )
    return result


# b) Top K sản phẩm bán chạy nhất tại thời điểm D (tháng-năm)
#    D dạng: "2021-01" hoặc "202101"
def top_k_products_in_month(df, k, D):
    k = int(k)
    ym = D.replace("-", "")  # "2021-01" -> "202101"
    if len(ym) != 6:
        raise ValueError("D phải có dạng YYYYMM hoặc YYYY-MM, ví dụ: 202101 hoặc 2021-01")

    df_month = df.filter(col("order_ym") == ym)

    result = (
        df_month.groupBy("ProductID", "ProductName")
                .agg(F_sum("Amount").alias("TotalAmount"))
                .orderBy(col("TotalAmount").desc())
                .limit(k)
    )
    return result


# c) Doanh thu bán hàng trên mỗi sản phẩm của toàn bộ hệ thống trong một năm
#    year: "2021"
def revenue_per_product_in_year(df, year):
    df_year = df.filter(col("order_year") == str(year))

    result = (
        df_year.groupBy("ProductID", "ProductName")
               .agg(F_sum("Revenue").alias("TotalRevenue"))
               .orderBy(col("TotalRevenue").desc())
    )
    return result


# d) Doanh thu bán hàng tại thời điểm D (tháng-năm) của mỗi shop
#    D: "2021-01" hoặc "202101"
def revenue_per_shop_in_month(df, D):
    ym = D.replace("-", "")
    if len(ym) != 6:
        raise ValueError("D phải có dạng YYYYMM hoặc YYYY-MM, ví dụ: 202101 hoặc 2021-01")

    df_month = df.filter(col("order_ym") == ym)

    result = (
        df_month.groupBy("ShopID")
                .agg(F_sum("Revenue").alias("TotalRevenue"))
                .orderBy(col("TotalRevenue").desc())
    )
    return result

def save_result_to_hdfs(df, path):
    (
        df.coalesce(1)  # demo cho dễ debug: 1 file CSV
          .write
          .mode("overwrite")
          .option("header", "true")
          .csv(path)
    )

def print_usage():
    print("""
Cách chạy:

  # a) Top K sản phẩm bán chạy nhất toàn hệ thống
  spark-submit orders_spark.py a <K>
    ví dụ: spark-submit orders_spark.py a 10

  # b) Top K sản phẩm bán chạy nhất tại tháng-năm D
  spark-submit orders_spark.py b <K> <D>
    ví dụ: spark-submit orders_spark.py b 5 2021-01

  # c) Doanh thu theo sản phẩm trong một năm
  spark-submit orders_spark.py c <YEAR>
    ví dụ: spark-submit orders_spark.py c 2021

  # d) Doanh thu theo shop tại tháng-năm D
  spark-submit orders_spark.py d <D>
    ví dụ: spark-submit orders_spark.py d 2021-01
""")


def main():
    if len(sys.argv) < 2:
        print_usage()
        sys.exit(1)

    mode = sys.argv[1]  # a, b, c, d

    spark = build_spark_session()
    df = load_orders_df(spark)

    if mode == "a":
        if len(sys.argv) != 3:
            print("Thiếu tham số K")
            print_usage()
            sys.exit(1)
        k = sys.argv[2]
        result = top_k_products(df, k)
        output_path = f"hdfs://namenode:8020/results/top_k_products_{k}"
    elif mode == "b":
        if len(sys.argv) != 4:
            print("Thiếu tham số K và D")
            print_usage()
            sys.exit(1)
        k = sys.argv[2]
        D = sys.argv[3]
        result = top_k_products_in_month(df, k, D)
        output_path = f"hdfs://namenode:8020/results/top_k_products_month_{D.replace('-', '')}"
    elif mode == "c":
        if len(sys.argv) != 3:
            print("Thiếu tham số YEAR")
            print_usage()
            sys.exit(1)
        year = sys.argv[2]
        result = revenue_per_product_in_year(df, year)
        output_path = f"hdfs://namenode:8020/results/revenue_per_product_{year}"
    elif mode == "d":
        if len(sys.argv) != 3:
            print("Thiếu tham số D")
            print_usage()
            sys.exit(1)
        D = sys.argv[2]
        result = revenue_per_shop_in_month(df, D)
        output_path = f"hdfs://namenode:8020/results/revenue_per_shop_{D.replace('-', '')}"
    else:
        print(f"Mode không hợp lệ: {mode}")
        print_usage()
        sys.exit(1)

    # In kết quả ra console (stdout)
    result.show(truncate=False)

    # Lưu ra HDFS để NiFi đọc
    save_result_to_hdfs(result, output_path)

    spark.stop()


if __name__ == "__main__":
    main()
