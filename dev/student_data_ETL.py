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


def clean_students(spark, logger, cur):

    # Create pyspark dataframe object using the queried information
    # using sqlite 3 since there is a pyspark configuration error that I cant fix 
    student_columns = ['uuid', 'name', 'dob', 'sex', 'contact_info', 'job_id', 'num_course_taken', 'current_career_path_id', 'time_spent_hrs']

    # Create pyspark dataframe object using the queried information
    student_df = spark.createDataFrame(cur.execute('''SELECT * FROM cademycode_students''').fetchall(),student_columns)

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

    for name,dtype in zip(sorted_columns,column_dtypes):
        clean_student_df = clean_student_df.withColumn(name, clean_student_df[name].cast(dtype))
    clean_student_df = clean_student_df.select(*sorted_columns)
    logger.debug(f'Final student_df results: {clean_student_df.take(5)}')

    return clean_student_df, student_df_missing_info

def clean_courses(spark, logger, cur):
    # Create list with column names
    course_columns = ['career_path_id', 'career_path_name', 'hours_to_complete']

    # Create pyspark dataframe using the data from the query
    course_df = spark.createDataFrame(cur.execute('''SELECT * FROM cademycode_courses''').fetchall(),course_columns)
    
    logger.debug(f'Final course_df results: {course_df.take(5)}')

    return course_df

def clean_jobs(spark, logger, cur):
    # Create list with column names
    job_columns = ['job_id', 'job_category', 'avg_salary']

    # Create PySpark dataframe from query
    job_df = spark.createDataFrame(cur.execute('''SELECT * FROM cademycode_student_jobs''').fetchall(),job_columns) 
    job_df = job_df.dropDuplicates()

    logger.debug(f'Final course_df results: {job_df.take(5)}')
    

    return job_df

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
    
    spark.sparkContext.setLogLevel('OFF')

    # Create sqlite connection and cursor
    con = sqlite3.connect(os.path.join('cademycode.db'))
    cur = con.cursor()

    student_df, missing_info_df = clean_students(spark,logger,cur)
    courses_df = clean_courses(spark,logger,cur)
    jobs_df = clean_jobs(spark,logger,cur)

if __name__ == "__main__":
    main()