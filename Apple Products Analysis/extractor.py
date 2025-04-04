# Databricks notebook source
# MAGIC %run "./reader_factory"

# COMMAND ----------

class Extractor:
    """
    Abstract class 
    """

    def __init__(self):
        pass
    def extract(self):
        pass

class AirpodsAfterIphoneExtractor(Extractor):

    def extract(self):
        """
        Implement the steps for extracting or reading the data
        """
        transcatioInputDF = get_data_source(
            data_type = "csv",
            file_path="dbfs:/FileStore/tables/Transaction_Updated.csv"
        ).get_data_frame()

        transcatioInputDF.orderBy("customer_id","transaction_date").display()

        customerInputDF = get_data_source(
            data_type = "delta",
            file_path="default.customer_delta_table_persist"
        ).get_data_frame()

        
        
        inputDFs = {
            "transcatioInputDF": transcatioInputDF,
            "customerInputDF": customerInputDF
        }

        return inputDFs
