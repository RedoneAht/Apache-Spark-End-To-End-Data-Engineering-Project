# Databricks notebook source
class Extractor:
    def __init__(self):
        pass

    def extract(self, path, type_file):
        """
        Extract the data from the volumes 
        """

        match type_file:
            case 'csv': 
                df = spark.read.format('csv').options(header=True,inferSchem=True).load(path)
            case 'json': 
                df = spark.read.format('json').load(path)
            case 'parquet': 
                df = spark.read.format('parquet').load(path)
            case 'delta': 
                df = spark.read.table(path)
            case _: 
                raise ValueError(f"Invalid file type: {type_file}") 
        
        return df