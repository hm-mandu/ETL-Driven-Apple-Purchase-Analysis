# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import lead, col, broadcast, collect_set, size, array_contains


class Transformer:
    def __init__(self):
        pass

    def transform(self, inputDFs):
        pass

class AirpodsAfterIphoneTransformer(Transformer):

    def transform(self, inputDFs):
        """
        Customers who have bought Airpods after buying the iPhone
        """

        transcatioInputDF = inputDFs.get("transcatioInputDF")

        print("transcatioInputDF in transform")

        transcatioInputDF.display()

        windowSpec = Window.partitionBy("customer_id").orderBy("transaction_date")

        transformedDF = transcatioInputDF.withColumn(
            "next_product_name", lead("product_name").over(windowSpec)
        )

        print("Airpods after buying iphone")
        transformedDF.orderBy("customer_id", "transaction_date", "product_name").display()

        filteredDF = transformedDF.filter(
            (col("product_name") == "iPhone") & (col("next_product_name") == "AirPods")
        )

        filteredDF.orderBy("customer_id", "transaction_date", "product_name").display()

        customerInputDF = inputDFs.get("customerInputDF")

        customerInputDF.display()

        joinDF =  customerInputDF.join(
           broadcast(filteredDF),
            "customer_id"
        )

        print("JOINED DF")
        joinDF.display()

        return joinDF.select(
            "customer_id",
            "customer_name",
            "location"
        )


class OnlyAirpodsAndIphone(Transformer):

    def transform(self, inputDFs):
        """
        Customer who have bought only iPhone and Airpods nothing else
        """

        transcatioInputDF = inputDFs.get("transcatioInputDF")

        print("transcatioInputDF in transform")

        groupedDF = transcatioInputDF.groupBy("customer_id").agg(
            collect_set("product_name").alias("products")
        )

        print("Grouped DF")
        groupedDF.display()

        filteredDF = groupedDF.filter(
            (array_contains(col("products"), "iPhone")) &
            (array_contains(col("products"), "AirPods")) & 
            (size(col("products")) == 2)
        )
        
        print("Only Airpods and iPhone")
        filteredDF.display()

        customerInputDF = inputDFs.get("customerInputDF")

        customerInputDF.display()

        joinDF =  customerInputDF.join(
           broadcast(filteredDF),
            "customer_id"
        )

        print("JOINED DF")
        joinDF.display()

        return joinDF.select(
            "customer_id",
            "customer_name",
            "location"
        )

        # 