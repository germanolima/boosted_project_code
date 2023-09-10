# %%
import json
import os
from functions.spark import config_spark
from functions.utils import load_warc_file_into_df, \
                            create_table_in_dw, \
                            table_exists_in_cz, \
                            get_delta_table_in_dw, \
                            merge_data_into_delta_table
import logging, pathlib, sys



logging_path = os.path.join(str(pathlib.Path(__file__).parent.resolve()), 'logs/etl.log')
logging.basicConfig(level=logging.INFO, filename=logging_path, format="%(asctime)s - %(levelname)s - %(message)s")
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

spark = config_spark()

# set source path folders
source_tables = ["CC-NEWS-20220101031644-00344.warc", "CC-NEWS-20220102205301-00365.warc"]

target_table_name = "CC-NEWS"
cwd = os.path.dirname(os.path.realpath(__file__))
parent_folder = os.path.abspath(os.path.join(cwd, os.pardir))

logging.info(f"Starting etl process for files {source_tables}")
for source_table in source_tables:

    logging.info(f"Loading data from warc file {source_table} into spark dataframe.")
    df = load_warc_file_into_df(parent_folder, source_table, spark)

    logging.info(f"Checking if table {target_table_name} already exists.")
    if table_exists_in_cz(spark, parent_folder, target_table_name) == False:
        logging.info(f"Table {target_table_name} does not exist. Creating table..")
        create_table_in_dw(df, parent_folder, target_table_name, spark)
    else:
        logging.info(f"Table {target_table_name} exists. Merging data.")
        delta_table = get_delta_table_in_dw(spark, parent_folder, target_table_name)
        merge_data_into_delta_table(df, delta_table, spark)
