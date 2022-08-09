import sqlalchemy
import joblib
import pg8000
import os
import re

from datetime import datetime

from pyspark.sql import DataFrame, SparkSession 
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.window import Window
from pyspark.sql import functions as F

from google.cloud import storage

# GCS paths
ENV = "dev"
BUCKET_NAME = f"capstone-project-{ENV}"

# Input
USER_PURCHASE_STAGE_PATH = \
    f"gs://{BUCKET_NAME}/Staging/user_purchase/"

MOVIE_REVIEW_STAGE_PATH = \
    f"gs://{BUCKET_NAME}/Staging/movie_review/"

LOG_REVIEW_STAGE_PATH = \
    f"gs://{BUCKET_NAME}/Staging/log_reviews/"

# Output
OBT_FINAL_PATH = \
    f"gs://{BUCKET_NAME}/Final/obt_table/"

OBT_NESTED_FINAL_PATH = \
    f"gs://{BUCKET_NAME}/Final/obt_nested_table/"


# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName("Data cleaning") \
    .getOrCreate()


def agg_movie_reviews():

    movie_review_stage = spark\
        .read\
        .parquet(MOVIE_REVIEW_STAGE_PATH)

    log_reviews_stage = spark\
        .read\
        .parquet(LOG_REVIEW_STAGE_PATH)

    movie_log_review = log_reviews_stage\
        .join(
            movie_review_stage, on='review_id', how='inner'
        )

    return movie_log_review


def agg_user_purchases():

    user_purchase_stage = spark\
        .read\
        .parquet(USER_PURCHASE_STAGE_PATH)
    
    customers_agg = user_purchase_stage\
        .groupBy('customer_id')\
        .agg(
            F.round(F.sum("amount"), 4).alias("amount_spent"),
            F.countDistinct("invoice_number").alias("count_invoices"),
            F.first("invoice_date").alias("min_invoice_date"),
            F.last("invoice_date").alias("max_invoice_date")
        )

    print(user_purchase_stage.count())
    print(customers_agg.count())

    return customers_agg


def denormalize_db():

    customer_cols = [
        'customer_id', 'amount_spent', 'count_invoices',
        'min_invoice_date', 'max_invoice_date',
    ]

    reviews_cols = [
        'log_date', 'season', 'device',
        'location', 'region', 'os', 'browser'
    ]
    movie_log_review = agg_movie_reviews()
    customers_agg = agg_user_purchases()

    movie_log_user_review = movie_log_review\
        .join(
            customers_agg, on='customer_id', how='inner'
        )

    obt_table = movie_log_user_review\
        .groupby(customer_cols + reviews_cols)\
        .agg(
            F.countDistinct(F.col("review_id")).alias("review_count"),
            F.sum(F.col("positive_review")).alias("review_score")
        )

    obt_nested_table = obt_table\
        .groupby(customer_cols)\
        .agg(
            F.collect_list(
                F.struct(
                    F.struct(
                        F.col("location"),
                        F.col("region"),
                    ).alias('location_info'),
                    F.struct(
                        F.col("logDate").alias('log_date'),
                        F.dayofmonth(F.to_date('logDate',  'MM-dd-yyyy')).alias('day'),
                        F.month(F.to_date('logDate',  'MM-dd-yyyy')).alias('month'),
                        F.year(F.to_date('logDate',  'MM-dd-yyyy')).alias('year'),
                        F.col('season')
                    ).alias('log_date_info'),
                    F.col("device"),
                    F.col("browser"),
                    F.col("os"),
                    F.col('review_count'),
                    F.col('review_score')
                )
            ).alias('reviews')
        )\
        .drop(
            "location", "logDate",  "device", "browser", "os"
        )
    # .orderBy(F.col('review_count').desc(), F.col('customer_id')).show(30, False)

    print(f"OBT table count: {obt_table.count()}")
    print(f"OBT nested table count: {obt_nested_table.count()}")

    obt_table\
        .write.mode('overwrite')\
        .parquet(OBT_FINAL_PATH)

    obt_nested_table\
        .write.mode('overwrite')\
        .parquet(OBT_NESTED_FINAL_PATH)


if __name__ == "__main__":
    denormalize_db()
