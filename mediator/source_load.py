from pyspark.sql.types import StructType,TimestampType 
import json
import os
from pyspark.sql.functions import col, from_json

from src.utils import get_logger, timer_func


class SourceLoader:
    """
    load the data sources and create a temporaty view for each sources
    the input: 
    - spark session instance
    """

    def __init__(self, spark_session, schema, logger=None):
        self.ss = spark_session
        self.schema = schema
        if logger != None:
            self.logger = logger
        else:
            self.logger = get_logger("source_load")

    @timer_func
    def build_struct(self, schema_obj, logger):

        schema = StructType()
        for column in schema_obj["columns"]:
            if column["name"]=="data_node_timestampUTC":
                schema.add(column["name"], TimestampType())
            else:
                schema.add(column["name"], column["type"])

        return schema

    def load(self, source_name) -> None:
        """
        Create temporary view for the source data designated by source_name in configs/locals.json  
        """
        with open(
            os.path.join("mediator/configs", self.schema, "locals.json")
        ) as f:
            config_local = json.load(f)
            struct_type_schema = self.build_struct(
                config_local[source_name]["schema"], 
                logger=self.logger
            )
            if config_local[source_name]["source"]["type"] == "file":
                df = self.load_file(
                    config_local[source_name]["source"]["details"],
                    struct_type_schema,
                    logger=self.logger
                )
            elif config_local[source_name]["source"]["type"] == "kafka":
                df = self.load_kafka(
                    config_local[source_name]["source"]["details"],
                    struct_type_schema,
                    logger=self.logger
                )
            self.create_view(
                df,
                source_name,
                config_local[source_name]["schema"]["geometry_column"],
                logger=self.logger
            )

    @timer_func
    def load_file(self, details, struct_type_schema, logger):
        """
        create dataframe from file with respect to local source configration
        """
        if details["format"] == "csv":
            df = (
                self.ss.read.option("delimiter", details["delimiter"])
                .option("header", True)
                .format(details["format"])
                .schema(struct_type_schema)
                .load(details["file_path"])
            )
            return df

    @timer_func
    def load_kafka(self, details, struct_type_schema, logger):
        """
        create dataframe from kafka topic with respect to local source configration
        """
        df = (
            self.ss.readStream.format("kafka")
            .option("kafka.bootstrap.servers", details["server"])
            .option("subscribe", details["topic"])
            .option("startingOffsets", "earliest")
            .load()
            .selectExpr("CAST(value AS STRING)")
        )
        df = df.withColumn(
            "value", from_json(col("value"), struct_type_schema)
        ).select("value.*")
        return df

    @timer_func
    def create_view(self, df, source_name, geom_column_name, logger) -> None:
        df.createOrReplaceTempView("df")
        df = self.ss.sql(
            "SELECT *, st_geomFromWKT("
            + geom_column_name
            + ") as geometry from df"
        )
        df = df.drop(col(geom_column_name))
        df.createOrReplaceTempView(source_name)
        self.logger.info(
            "schema of DataFrame {} is : {}".format(source_name, df.schema)
        )
