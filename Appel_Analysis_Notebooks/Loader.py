# Databricks notebook source
class Loader:
    def __init__(self):
        pass

    def load(self, df, load_type, mode, path, params=None):
        """
        Load the data to the volumes 
        """

        match load_type:
            case 'dbfs': 
                df.write.mode(mode).save(path)
            case 'dbfs_with_partition': 
                df.write.mode(mode).partitionBy(params).save(path)
            case _: 
                raise ValueError(f"Invalid load type: {load_type}") 