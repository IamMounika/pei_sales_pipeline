
from databricks.connect import DatabricksSession
from datetime import datetime
from pyspark.sql.functions import col,regexp_replace, isnan, udf, year, when, substring_index
from pyspark.sql.types import IntegerType, StringType, StructType, StructField,FloatType, DateType

# # Initialize Spark Session
spark = DatabricksSession.builder.getOrCreate()


def load_raw_data(spark, filepath, fileformat, schema):
    try:
        if fileformat == 'json':
            df = spark.read.format(fileformat).option("multiline", "true").load(filepath)
        elif fileformat == 'csv':
            df = spark.read.format(fileformat).schema(schema).option("header", True).load(filepath)
        else:
            df = spark.read.format(fileformat).option("header", "true").option("treatEmptyValuesAsNulls", "false").option("inferSchema", "false")
        return df
    except FileNotFoundError as e:
        print(f"File not found: {str(e)}")
        return None
    except Exception as e:
        print(f"An error occurred while loading data: {str(e)}")
        return None


#defining schema for product table

product_schema = StructType(fields=
                            [
                                StructField("product_id",StringType(),False),
                                StructField("category",StringType(),True),
                                StructField("sub_category",StringType(),True),
                                StructField("product_name",StringType(),True),
                                StructField("state",StringType(),True),
                                StructField("price_per_product",FloatType(),True)
                            ]
                            )

customer_schema = StructType(fields=
                            [
                                StructField("customer_id",StringType(),False),
                                StructField("customer_name",StringType(),True),
                                StructField("email",StringType(),True),
                                StructField("phone",StringType(),True),
                                StructField("address",StringType(),True),
                                StructField("segment",StringType(),True),
                                StructField("country",StringType(),True),
                                StructField("city",StringType(),True),
                                StructField("state",StringType(),True),
                                StructField("postal_code",StringType(),True),
                                StructField("region",StringType(),True)
                            ]
                            )

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

order_schema2 = StructType(fields=
                            [
                                StructField("Row ID",IntegerType(),False),
                                StructField("Order ID",StringType(),True),
                                StructField("Order Date",DateType(),True),
                                StructField("Ship Date",DateType(),True),
                                StructField("Ship Mode",DateType(),True),
                                StructField("Customer ID",StringType(),True),
                                StructField("Product ID",StringType(),True),
                                StructField("Quantity",IntegerType(),True),
                                StructField("Price",FloatType(),True),
                                StructField("Discount",FloatType(),True),
                                StructField("Profit",FloatType(),True)
                            ]
                            )                                                     


filepath_base = '/Volumes/test_databricks_workspace/pei_schema/pei_volume/'
order_filepath = filepath_base + "Order.json"
product_filepath = filepath_base + "Product.csv"
customer_filepath = filepath_base + "Customer.xlsx"

#udf for date formatting
string_date_udf =  udf (lambda x: datetime.strptime(x, '%d/%m/%Y'), DateType())

#cleansing, casting and formatting
def transform_enrich_order_dataframe():
    df_order_raw = load_raw_data(spark,order_filepath,'json',order_schema)
    df_order_raw_casted = df_order_raw.select(col("Order ID").alias("order_id"), col("Order Date").alias("order_date"), col("Ship Date").alias('ship_date'), col("Ship Mode").alias("ship_mode"), col("Customer ID").alias("customer_id"), col("Product ID").alias("product_id"), col("Quantity"),col("price"), col("Discount"), col("profit"))
    df_order_date_conversion = df_order_raw_casted.withColumn('order_date',string_date_udf(col('order_date'))).withColumn('ship_date', string_date_udf(col('ship_date')))
    df_order_enriched = df_order_date_conversion.withColumn('year',year(col('order_date')))
    return df_order_enriched
    
def transform_enrich_customer_dataframe(): 
    df_customer_raw = load_raw_data(spark,order_filepath,'com.crealytics.spark.excel')
    df_customer_renamed = df_customer_raw.select(col("customer id").alias("customer_id"), col("customer name").alias("customer_name"), col("email"), col("phone").cast("double"),col("address"), col("segment"), col("country"), col("state"), col("city"), col("postal code").alias("postal_code"), col("region"))
    df_customer_modified = df_customer_renamed.withColumn("customer_name", 
                                             when(isnan(col("customer_name")), substring_index(col("email"), "@", 1))
                                             .otherwise(col("customer_name"))).withColumn("customer_name", regexp_replace(col("customer_name"), '[^a-zA-Z\s]',''))   
    return df_customer_modified

def tranform_enrich_product_dataframe():
    df_product_raw = load_raw_data(spark,order_filepath,'csv', product_schema)
    return df_product_raw

def join():
    # preparing for join operation
    df_customer_required = df_customer_modified.select(col('customer_id'),col('customer_name'),col('country').alias('customer_country'))

    df_product_required = df_product_raw.select(col('product_id'),col('category'),col('sub_category'))

    df_joined = df_order_enriched.join(df_product_required, 'product_id').join(df_customer_required,'customer_id')


    df_joined = df_joined.withColumn('profit',round(col('profit'),2))
    return df_joined

def aggregations(df_joined):
    df_agg1 = df_joined.groupby('year').agg({"profit": "sum"})
    df_agg1 = df_agg1.withColumnRenamed("sum(profit)", "year_profits")

    df_agg2 = df_joined.groupby('customer_id').agg({"profit": "sum"})
    df_agg2 = df_agg2.withColumnRenamed("sum(profit)", "customer_profits")

    df_agg3 = df_joined.groupby('category').agg({"profit": "sum"})
    df_agg3 = df_agg3.withColumnRenamed("sum(profit)", "category_profits")

    df_agg4 = df_joined.groupby('sub_category').agg({"profit": "sum"})
    df_agg4 = df_agg4.withColumnRenamed("sum(profit)", "sub_category_profits")

    return df_agg1, df_agg2, df_agg3, df_agg4


def agg_with_year_category():
    # Convert DataFrame to temp table in Spark
    df_joined.createOrReplaceTempView("final_table")

    # SQL query to fetch sum of profits grouped by year and category
    sql_query_year_category = '''
    SELECT year, category, SUM(profit) AS sum_profits
    FROM final_table
    GROUP BY year, category 
    ORDER BY year
    '''

    # Execute the SQL query
    df_year_category = spark.sql(sql_query_year_category)  

    return df_year_category


def agg_with_year_customer():

    # SQL query to fetch sum of profits grouped by year and category
    sql_query_year_customer = '''
    SELECT year, customer_id, SUM(profit) AS sum_profits
    FROM final_table
    GROUP BY year, customer_id 
    ORDER BY year
    '''

    # Execute the SQL query
    df_year_customer = spark.sql(sql_query_year_customer)

    return df_year_customer

if __name__ == "main":
    df_order_enriched = transform_enrich_order_dataframe()
    df_customer_modified = transform_enrich_customer_dataframe()
    df_product_raw = transform_enrich_customer_dataframe()

    df_joined = join()

    aggregations(df_joined)
    df_year_category = agg_with_year_category()
    df_year_customer = agg_with_year_customer()

