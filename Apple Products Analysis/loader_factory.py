# Databricks notebook source
class DataSink:
    """
    Abstract Class
    """
    def __init__(self, df, path, method, params):
        self.df = df
        self.path = path
        self.method = method
        self.params = params
    
    def load_data_frame(self):
        """
        Abstract method. Functions will be defined in subclasses.
        """
        raise ValueError("Not Implemented")

class LoadToDbfs(DataSink):
    def load_data_frame(self):
        self.df.write.mode(self.method).save(self.path)

class LoadToDbfsWithPartition(DataSink):
    def load_data_frame(self):
        partitionByColumns = self.params.get("partitionByColumns")
        self.df.write.mode(self.method).partitionBy(*partitionByColumns).save(self.path)

class LoadToDeltaTable(DataSink):
    def load_data_frame(self):
        
        self.df.write.format("delta").mode(self.method).saveAsTable(self.path)

def get_sink_source(sink_type, df, path, method, params=None):
    if sink_type == "dbfs":
        return LoadToDbfs(df, path, method, params)
    elif sink_type == "dbfs_with_partition":
        return LoadToDbfsWithPartition(df, path, method, params)
    elif sink_type == "delta":
        return LoadToDeltaTable(df, path, method, params)
    else:
        return ValueError(f"Not implemented for sink type: {sink_type}")
