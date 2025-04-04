# Databricks notebook source

class DataSource:
    """
    Abstract Class
    """
    def __init__(self, path):
        self.path = path
    
    def get_data_frame(self):
        """
        Abstract method. Functions will be defined in subclasses.
        """
        raise ValueError("Not Implemented")

class CsvDataSource(DataSource):
    def get_data_frame(self):
        return (
            spark
            .read
            .format('csv')
            .option('header', True)
            .load(self.path)
        )

class ParquetDataSource(DataSource):
    def get_data_frame(self):
        return (
            spark
            .read
            .format('parquet')
            .load(self.path)
        )

class DeltaDataSource(DataSource):
    def get_data_frame(self):

        table_name = self.path
        return (
            spark
            .read
            .table(table_name)     
        )

def get_data_source(data_type, file_path):
    if data_type == 'csv':
        return CsvDataSource(file_path)
    elif data_type == 'parquet':
        return ParquetDataSource(file_path)
    elif data_type == 'delta':
        return DeltaDataSource(file_path)
    else:
        raise ValueError(f"Not implemented for data type: {data_type}")