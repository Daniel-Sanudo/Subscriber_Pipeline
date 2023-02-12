import logging
import sqlite3
import os
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, DateType, StringType

logging.basicConfig(filename="cleanse_db.log",
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                    filemode='w',
                    level=logging.DEBUG)
logger = logging.getLogger(__name__)


def clean_students(spark, logger):
    # Create pyspark dataframe object using the queried information
    student_df = spark.read \
                .format('jdbc') \
                .option('url','jdbc:sqlite:cademycode.db') \
                .option('query', 'SELECT * FROM cademycode_students') \
                .option('inferSchema', True) \
                .load()

    # Show the dtypes which were assigned
    logger.debug(f'Read the following columns and dtypes: {student_df.dtypes}')

    # Create a new pyspark dataframe which contains the following:
    # uuid to use in the join
    # mailing address which is extracted from the dictionary
    # email address which is extracted from the dictionary
    
    student_address_df = student_df.select(
                    student_df.uuid.alias('student_uuid'),
                    f.get_json_object(student_df.contact_info.cast(StringType()), '$.mailing_address').alias("mailing_address"),
                    f.get_json_object(student_df.contact_info.cast(StringType()), '$.email').alias("email_address"))
                    
    student_df.drop('contact_info')

    logger.debug(student_address_df.dtypes)

    # Split the address string into separate parts
    student_address_df = student_address_df.withColumn("split_col", f.split(student_address_df["mailing_address"], ","))

    # Combine the first three elements of the split_col list into the "street" column
    student_address_df = student_address_df.withColumn("street", f.array_join(f.slice(student_address_df["split_col"],1,1),' '))

    # Extract the city, state, and zipcode from the split_col list and create separate columns
    student_address_df = student_address_df.withColumn("city", f.array_join(f.slice(student_address_df["split_col"],2,1),' '))
    student_address_df = student_address_df.withColumn("state", f.array_join(f.slice(student_address_df["split_col"],3,1),' '))
    student_address_df = student_address_df.withColumn("zipcode", f.array_join(f.slice(student_address_df["split_col"],4,1),' '))

    # Drop the split_col column
    student_address_df = student_address_df.drop("split_col")

    # Drop the mailing address column
    student_address_df = student_address_df.drop("mailing_address")

    # Join the contact information dataframe to the original dataframe and drop the join column to avoid duplicity
    clean_student_df = student_df.join(student_address_df, student_df.uuid == student_address_df.student_uuid).drop('student_uuid')

    logger.debug(f'Resulting columns and dtypes: {clean_student_df.dtypes}')

    # Extract the year from the birthdate and store it as its own value to make it easier to access it for further analytics
    clean_student_df = clean_student_df.withColumn('birth_year', f.array_join(f.slice(f.split(clean_student_df.dob, '-'),1,1),''))

    # Create a new dataframe with the missing information instead of deleting it so it can be used to look into the reasons for missing data
    student_df_missing_info = clean_student_df.exceptAll(clean_student_df.dropna())

    # Drop the null/nan/missing values from the dataframe now that they have been stored in another
    clean_student_df = clean_student_df.dropna()

    logger.debug(f'Resulting columns and dtypes after dropping NAs:: {clean_student_df.dtypes}')

        # Sort the student_df columns and cast them to the right dtype
    sorted_columns = ['uuid', 'name', 'dob', 
                    'birth_year', 'sex', 'email_address',
                    'street', 'city', 'state', 'zipcode',
                    'job_id', 'num_course_taken', 'current_career_path_id',
                    'time_spent_hrs']
    column_dtypes = [IntegerType(), StringType(), DateType(),
                    IntegerType(), StringType(), StringType(),
                    StringType(), StringType(), StringType(), IntegerType(),
                    IntegerType(), IntegerType(), IntegerType(),
                    FloatType()]

    for key,value in zip(sorted_columns,column_dtypes):
        clean_student_df = clean_student_df.withColumn(key, clean_student_df[key].cast(value))
    clean_student_df = clean_student_df.select(*sorted_columns)
    logger.debug(f'Final student_df results: {clean_student_df.columns}')
    logger.debug(f'Final student_df results: {clean_student_df.show(10)}')

def clean_courses():
    pass

def clean_jobs():
    pass 

def main():
    # Create spark session
    spark = SparkSession.builder \
        .appName('ETL') \
        .master('local[*]') \
        .config(
        "spark.jars",
        "{}/sqlite-jdbc-3.34.0.jar".format(os.getcwd())) \
        .config(
        "spark.driver.extraClassPath",
        "{}/sqlite-jdbc-3.34.0.jar".format(os.getcwd())) \
        .getOrCreate()
        
    clean_students(spark,logger)

if __name__ == "__main__":
    main()