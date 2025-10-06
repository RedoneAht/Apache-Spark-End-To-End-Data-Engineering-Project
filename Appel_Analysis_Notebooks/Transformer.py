# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

class Firsttransformer:
    def __init__(self):
        pass

    def transform(self, input_dfs):
        """
        Show customers who have bought Airpods just after bying Iphone
        """
        ## Retrieve the dataframes
        customer_df = input_dfs["df_customer"]
        transaction_df = input_dfs["df_transaction"]

        ## Add the column for the name of the next product bought
        Transformed_df= transaction_df.withColumn(
            "next_product_name", 
            lead("product_name").over(Window.partitionBy("customer_Id").orderBy("customer_Id","transaction_date"))
        )

        ## Find the customers who bought Airpods after iPhone
        filtered_df = Transformed_df.filter(
                    (col('product_name') == 'iPhone') & (col("next_product_name") == 'AirPods')
                )
        
        ## Show the infos of those customers from the customer table
        customer_ids = [row.customer_id for row in filtered_df.select("customer_id").collect()]

        result_df = customer_df.filter(col("customer_id").isin(customer_ids))

        return result_df

# COMMAND ----------

class Secondtransformer:
    def __init__(self):
        pass

    def transform(self, input_dfs):
        """
        Show customers who have bought only iphone and airpods
        """
        ## Retrieve the dataframes
        customer_df = input_dfs["df_customer"]
        transaction_df = input_dfs["df_transaction"]

        ## Find the list of products bought by every customer (collect_set removes duplicate)
        list_products_per_customer = transaction_df.groupBy('customer_id').agg(collect_set('product_name').alias('products'))

        ## Find the customers who bought only Airpods and iPhone
        desired_customers_ids = list_products_per_customer.filter(
            array_sort(col("products")) == array_sort(array(lit("iPhone"), lit("AirPods")))
        )
        

        ## Show the infos about those customers from the customer table
        customer_ids = [row.customer_id for row in desired_customers_ids.select("customer_id").collect()]

        result_df = customer_df.filter(col("customer_id").isin(customer_ids))

        return result_df