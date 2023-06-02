"""
Adds geolocation information for our collected ip address

Schema:
  `ip_address`: ip address from our page view data
  `processed_dttm`: current time
  `geo_raw`: raw json response from geolocation-db.com api

"""

import pyspark.pandas as ps
from pyspark.sql.functions import udf
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import col
from pyspark.sql.types import StringType

import requests


def get_response(url):
  res = requests.get(url).text
  return res

def itter_funct(row):
  return get_response(row.rest_endpoint)

def model(dbt, session):
  dbt.config(materialized = "incremental")

  df = dbt.ref("wrk_event_ip_address_to_process")
  df = df.rdd.map(itter_funct)
  df = df.toDF()

  df = df.withColumn("processed_dttm", current_timestamp())

  return df
