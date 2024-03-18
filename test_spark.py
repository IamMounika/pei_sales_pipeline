from pyspark.sql import SparkSession
import pytest
from pyspark.sql.functions import col,expr,regexp_replace, isnan, to_date, date_format, udf, year, when, substring_index
from pyspark.sql.types import IntegerType, StringType, StructType, StructField,FloatType, DoubleType, DateType

from data_tranformations import *

@pytest.fixture
def spark() -> SparkSession:
  # Create a SparkSession (the entry point to Spark functionality) on
  # the cluster in the remote Databricks workspace. Unit tests do not
  # have access to this SparkSession by default.
  return SparkSession.builder.getOrCreate()

# Now add your unit tests.

# For example, here is a unit test that must be run on the
# cluster in the remote Databricks workspace.
# This example determines whether the specified cell in the
# specified table contains the specified value. For example,
# the third column in the first row should contain the word "Ideal":
#
# +----+-------+-------+-------+---------+-------+-------+-------+------+-------+------+
# |_c0 | carat | cut   | color | clarity | depth | table | price | x    | y     | z    |
# +----+-------+-------+-------+---------+-------+-------+-------+------+-------+------+
# | 1  | 0.23  | Ideal | E     | SI2     | 61.5  | 55    | 326   | 3.95 | 3. 98 | 2.43 |
# +----+-------+-------+-------+---------+-------+-------+-------+------+-------+------+
# ...
#
def test_spark(spark):
  spark.sql('USE default')
#   data = spark.sql('SELECT * FROM diamonds')
#   assert data.collect()[0][2] == 'Ideal'
  assert 1+1 == 2


def test_sample():
  count = sample()
  assert count == 9990


order_schema = StructType(fields=
                            [
                                StructField("row_id",IntegerType(),False),
                                StructField("order_id",StringType(),True),
                                StructField("order_date",DateType(),True),
                                StructField("ship_date",DateType(),True),
                                StructField("ship_mode",DateType(),True),
                                StructField("customer_id",StringType(),True),
                                StructField("product_id",StringType(),True),
                                StructField("quantity",IntegerType(),True),
                                StructField("price",FloatType(),True),
                                StructField("discount",FloatType(),True),
                                StructField("profit",FloatType(),True)
                            ]
                            )      

# Verifying if an error is raised when trying to load data from an invalid path
def test_invalid_path(spark):    
  test_file_path = '/Volumes/test_databricks_workspace/pei_schema/Sample_Order.json'
  with pytest.raises(Exception):
    df_order_raw = load_raw_data(spark,test_file_path,'json', order_schema)
    df_order_raw.count()

# verifying the data_load() method with sample file
def test_load_raw_data(spark):
  test_file_path = '/Volumes/test_databricks_workspace/pei_schema/pei_volume/Sample_Order.json'
  df_order_raw = load_raw_data(spark,test_file_path,'json', order_schema)
  assert df_order_raw.count() == 7

# check for the data type casting
def test_date_type():
  df_order_enriched= transform_enrich_order_dataframe()
  assert dict(df_order_enriched.dtypes)['order_date'] == 'date'

  
# check if the data join is done
def test_join():
  df_joined = join()
  assert len(df_joined.columns) == 15


def test_aggregations():
  df_joined = join()
  df_year_profits,_ = aggregations(df_joined)
  row = df_year_profits.filter(col("year")=='2014')
  assert row['year_profits'] == 40975.530000000006