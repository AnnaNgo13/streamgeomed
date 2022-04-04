import json
import os
from icecream import ic
from pyspark.sql.functions import *
from sedona.register import SedonaRegistrator
from pyspark.sql import SparkSession


from mediator.source_load import SourceLoader
from src.utils import get_logger, timer_func
from mediator.query_parser import Parser

class Wrapper():
    
    def __init__(self, schema, app_name, spark_master = "spark://127.0.0.1:7077",logger=None) -> None:
        self.spark = (
            SparkSession.builder.master(spark_master)
            .appName(app_name)
            .config('spark.jars.packages',
                'org.apache.sedona:sedona-python-adapter-3.0_2.12:1.1.1-incubating,'
                'org.datasyslab:geotools-wrapper:1.1.0-25.2,'
                'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2')
            .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("WARN")
        SedonaRegistrator.registerAll(self.spark)
        self.schema = schema
        if logger == None:
            self.logger = get_logger("wrapper")
        else: 
            self.logger = logger

    def get_spark(self):
        return self.spark
     

    @timer_func
    def load_global_view(self, tables):
        """
        Load each table of the global schema into a dataframe
        """
        with open(os.path.join("mediator/configs", self.schema, "globals.json")) as f:
            config_global = json.load(f)
            ic(config_global)
            self.logger.info("Schema: {}".format(self.schema))
        # load sources
        sourceLoader = SourceLoader(self.spark, self.schema, self.logger)
        df_dict = dict()
        for table in tables:
            for source in config_global[table]["sources"]:
                sourceLoader.load(source)
            # run transformation (mapping) query to get the dataframe of the table in global schema and then create view from it
            df = self.spark.sql(config_global[table]["transformation"])
            df.createOrReplaceTempView(table)
            df_dict[table] = df
        
        return df_dict
