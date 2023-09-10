from warcio.archiveiterator import ArchiveIterator
import datetime
import os
from delta.tables import *

def load_warc_file_into_df(
    parent_folder: str,
    table_name: str,
    spark
):
    with open(f"{parent_folder}/cc_dump/{table_name}", 'rb') as stream:
        insert_list = []
        for record in ArchiveIterator(stream):
            if record.rec_type == 'response':    
                insert_obj = {
                    "WARC-Record-ID": record.rec_headers.get_header('WARC-Record-ID'),
                    "Content-Length": record.rec_headers.get_header('Content-Length'),
                    "WARC-Date": record.rec_headers.get_header('WARC-Date'),
                    "WARC-Type": record.rec_headers.get_header('WARC-Type'),
                    "Content-Type": record.rec_headers.get_header('Content-Type'),
                    "WARC-Payload-Digest": record.rec_headers.get_header('WARC-Payload-Digest'),
                    "WARC-Block-Digest": record.rec_headers.get_header('WARC-Block-Digest'),
                }            
                insert_list.append(insert_obj)
        
        df = spark.createDataFrame(insert_list)

    return df
                
def create_table_in_dw(
    df,
    parent_folder: str,
    table_name: str,
    spark
):  
    df.createOrReplaceTempView("SAVE_TABLE")
    df_save = spark.sql("""
        SELECT
            *,
            CAST(`WARC-Date` AS DATE) AS partition_date
        FROM
            SAVE_TABLE
    """)
    df_save.write.format("delta").partitionBy("partition_date").save(os.path.join(parent_folder, "dw", table_name))
    return True

def table_exists_in_cz(
    spark,
    parent_folder: str,
    table_name: str
) -> bool:
    return DeltaTable.isDeltaTable(spark, os.path.join(parent_folder, "dw", table_name))

def get_delta_table_in_dw(
    spark,
    parent_folder: str,
    table_name: str
):
    delta_table = DeltaTable.forPath(spark, os.path.join(parent_folder, "dw", table_name))
    return delta_table

def merge_data_into_delta_table(
    df,
    delta_table,
    spark
):
    
    df.createOrReplaceTempView("MERGE_TABLE")
    df_merge = spark.sql("""
        SELECT
            *,
            CAST(`WARC-Date` AS DATE) AS partition_date
        FROM
            MERGE_TABLE
    """)

    delta_table.alias('delta') \
    .merge(
        df_merge.alias('updates'),
        'delta.`WARC-Record-ID` = updates.`WARC-Record-ID`'
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()