from functions.spark import config_spark
from fastapi import FastAPI, Query
from typing_extensions import Annotated
from pyspark.sql import functions as F
import os

app = FastAPI()

spark = config_spark()

cwd = os.path.dirname(os.path.realpath(__file__))
parent_folder = os.path.abspath(os.path.join(cwd, os.pardir))

@app.get("/query")
async def query_table(table_name: str, date: str):

    df = spark.read.format("delta").load(f"{parent_folder}/dw/{table_name}").filter(F.col("partition_date")==F.lit(date))

    return df.toJSON().collect()
