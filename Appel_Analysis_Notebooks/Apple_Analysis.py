# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Apple_Analysis").getOrCreate()

# COMMAND ----------

# MAGIC %run "/Users/ahntateridwane@gmail.com/AppelAnalysis/Extractor"

# COMMAND ----------

# MAGIC %run "/Users/ahntateridwane@gmail.com/AppelAnalysis/Transformer"

# COMMAND ----------

# MAGIC %run "/Workspace/Users/ahntateridwane@gmail.com/AppelAnalysis/Loader"

# COMMAND ----------

Extractor = Extractor()
Firsttransformer = Firsttransformer()
Secondtransformer = Secondtransformer() 
Loader = Loader()

# COMMAND ----------

class FirstWorkflow:
    """
    ETL pipeline for generating the data for the customers who have bought Airpods just after bying Iphone"""
    def __init__(self):
        pass

    def run(self):
        ## Extract the data from the sources
        print("Extracting the client data...")
        df_customer = Extractor.extract("apple_analysis.default.customer_table", 'delta')
        df_customer.display()

        print("Extracting the transactions data...")
        df_transaction = Extractor.extract("/Volumes/apple_analysis/default/apple_anlaysis_data/Transaction_Updated.csv", 'csv')
        df_transaction.display()

        ## Transform the data
        input_dfs ={
            "df_customer": df_customer,
            "df_transaction": df_transaction
        }
        ## customers who bought iphone then airpods
        print("customers who bought iphone then airpods:")
        df_transformed = Firsttransformer.transform(input_dfs)
        df_transformed.display()

        ## Load the data
        print("Loading the data...")
        Loader.load(df_transformed, "dbfs", 'overwrite', '/Volumes/apple_analysis/default/apple_analysis_loaded_data/AirPods_After_iPhone/')
        print("Data loaded successfully!")

# COMMAND ----------

class SecondWorkflow:
    """
    ETL pipeline for generating the data for the customers who have bought only Airpods and Iphone"""
    def __init__(self):
        pass

    def run(self):
        ## Extract the data from the sources
        print("Extracting the client data...")
        df_customer = Extractor.extract("apple_analysis.default.customer_table", 'delta')
        df_customer.display()

        print("Extracting the transactions data...")
        df_transaction = Extractor.extract("/Volumes/apple_analysis/default/apple_anlaysis_data/Transaction_Updated.csv", 'csv')
        df_transaction.display()

        ## Transform the data
        input_dfs ={
            "df_customer": df_customer,
            "df_transaction": df_transaction
        }
        ## ccustomers who bought only iphone and airpods
        print("customers who bought only iphone and airpods:")
        df_transformed = Secondtransformer.transform(input_dfs)
        df_transformed.display()

        ## Load the data
        print("Loading the data...")
        Loader.load(df_transformed, "dbfs_with_partition", 'overwrite', '/Volumes/apple_analysis/default/apple_analysis_loaded_data/Only_AirPods_And_iPhone/', 'location')
        print("Data loaded successfully!")

# COMMAND ----------

class WorkflowRunner:
    def __init__(self, name):
        self.name = name
    
    def runner(self):
        if self.name == "FirstWorkflow":
            return FirstWorkflow().run()
        elif self.name == "SecondWorkflow":
            return SecondWorkflow().run()
        else:
            raise ValueError(f"Not Implemented workflow {self.name}")


# COMMAND ----------

WorkflowRunner("FirstWorkflow").runner()
#WorkflowRunner("SecondWorkflow").runner()