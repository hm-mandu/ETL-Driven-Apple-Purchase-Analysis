# Databricks notebook source
# MAGIC %run "./transform"

# COMMAND ----------

# MAGIC %run "./extractor "

# COMMAND ----------

# MAGIC %run "./loader"

# COMMAND ----------


class FirstWorkFlow:
    """
    ETL pipeline to generate the data for all customers who have bought Airpods just after buying iPhone
    """ 
    def __init__(self):
        pass

    def runner(self):

        # Step 1: Extract all required data from different source
        inputDFs = AirpodsAfterIphoneExtractor().extract()

        # Step 2: Implement the Transformation logic
        # Customers who have bought Airpods after buying the iPhone
        firstTransformedDF = AirpodsAfterIphoneTransformer().transform(inputDFs)

        # Step 3: Load all required data to differnt sink
        AirPodsAfterIphoneLoader(firstTransformedDF).sink()

# COMMAND ----------

class SecondWorkFlow:
    """
    ETL pipeline to generate the data for all customers who have bought only iPhone and Customers
    """ 
    def __init__(self):
        pass

    def runner(self):

        # Step 1: Extract all required data from different source
        inputDFs = AirpodsAfterIphoneExtractor().extract()

        # Step 2: Implement the Transformation logic
        # Customers who have bought Airpods after buying the iPhone
        onlyAirpodsAndIphoneDF = OnlyAirpodsAndIphone().transform(inputDFs)

        # Step 3: Load all required data to differnt sink
        OnlyAirpodsAndIPhoneLoader(onlyAirpodsAndIphoneDF).sink()

# COMMAND ----------


class WorkFlowRunner:

    def __init__(self, name):
        self.name = name

    def runner(self):
        if self.name == "firstWorkFlow":
            return FirstWorkFlow().runner()
        elif self.name == "secondWorkFlow":
            return SecondWorkFlow().runner()
        else:
            raise ValueError(f"Not Implemented for {self.name}")

name = "secondWorkFlow" 

workFlowrunner = WorkFlowRunner(name).runner()

# COMMAND ----------

# from pyspark.sql import SparkSession

# spark = SparkSession.builder.appName("apple.me").getOrCreate()

# input_df = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/tables/Transaction_Updated.csv")

# input_df.display()
