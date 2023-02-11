import logging
import sqlite3
import os
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, FloatType, DateType, StringType

logging.basicConfig(filename="cleanse_db.log",
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                    filemode='w',
                    level=logging.DEBUG,
                    force=True)
logger = logging.getLogger(__name__)


def clean_students():
    pass

def clean_courses():
    pass

def clean_jobs():
    pass 

def main():
    # Create spark session
    spark = SparkSession.builder \
        .appName('EDA') \
        .master('local[*]') \
        .config(
        "spark.jars",
        "{}/sqlite-jdbc-3.34.0.jar".format(os.getcwd())) \
        .config(
        "spark.driver.extraClassPath",
        "{}/sqlite-jdbc-3.34.0.jar".format(os.getcwd())) \
        .getOrCreate()

    