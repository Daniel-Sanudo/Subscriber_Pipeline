import logging
import sqlite3
import os
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, FloatType, DateType, StringType

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
    logger.debug(f'Final student_df dtypes: {clean_student_df.dtypes}')

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

def test_job_id_match(student_df,job_df):
    student_df_job_ids = student_df.select('job_id').dropDuplicates()
    job_df_job_ids = job_df.select('job_id').dropDuplicates()
    try:
        test_result = student_df_job_ids.subtract(job_df_job_ids)
        assert test_result.count() == 0, "Job IDs between student and job df do not match"
    except AssertionError as ae:
        logger.exception(ae)
        logger.exception(f'Mismatch with the following job ids: {test_result}')
        raise ae
    else:
        logger.debug('All job ids are present')

def test_course_id_match(student_df, course_df):
    student_df_course_ids = student_df.select('current_career_path_id').dropDuplicates()
    course_df_course_ids = course_df.select('career_path_id').dropDuplicates()
    try:
        test_result = student_df_course_ids.subtract(course_df_course_ids) 
        assert test_result.count() == 0, "Course IDs between student and course df do not match"
    except AssertionError as ae:
        logger.exception(ae)
        logger.exception(f'Mismatch with the following job ids: {test_result}')
        raise ae
    else:
        logger.debug('All career ids are present')

def test_no_course_nulls(course_df):
    for col in course_df.columns:
        try: 
            nulls = course_df.filter(course_df[col].isNull()).count()
            assert nulls == 0, 'Null values found in course_df'
        except AssertionError as ae:
            logger.exception(ae)
            logger.exception(f'Null values found in {col} from course_df')
            raise ae
        else:
            logger.debug(f'No nulls in {col}')

def test_no_job_nulls(job_df):
    for col in job_df.columns:
        try: 
            nulls = job_df.filter(job_df[col].isNull()).count()
            assert nulls == 0, 'Null values found in job_df'
        except AssertionError as ae:
            logger.exception(ae)
            logger.exception(f'Null values found in {col} from job_df')
            raise ae
        else:
            logger.debug(f'No nulls in {col}')

def test_no_student_nulls(student_df):
    for col in student_df.columns:
        try: 
            nulls = student_df.filter(student_df[col].isNull()).count()
            assert nulls == 0, 'Null values found in student_df'
        except AssertionError as ae:
            logger.exception(ae)
            logger.exception(f'Null values found in {col} from student_df')
            raise ae
        else:
            logger.debug(f'No nulls in {col}')

def test_no_negative_values(student_df):
    min_course_count = student_df.agg({'num_course_taken':'min'}).collect()[0][0]
    logger.debug(f'Comparing {min_course_count} as min_course_count')
    min_hours_spent = student_df.agg({'time_spent_hrs':'min'}).collect()[0][0]
    logger.debug(f'Comparing {min_hours_spent} as min_hours_spent')
    try:
        assert min_course_count >= 0, 'Value below 0 found in num_course_taken'
    except AssertionError as ae:
        logger.exception(ae)
        logger.exception(f'Negative value found in student_df.num_course_taken')
    try:
        assert min_hours_spent >= 0, 'Value below 0 found in time_spent_hrs'
    except AssertionError as ae:
        logger.exception(ae)
        logger.exception(f'Negative value found in student_df.time_spent_hrs')
        raise ae

def test_student_df_dtypes(student_df):
    current_dtype_list = student_df.dtypes
    student_columns = ['uuid', 'name', 'dob', 
                    'birth_year', 'sex', 'email_address',
                    'street', 'city', 'state', 'zipcode',
                    'job_id', 'num_course_taken', 'current_career_path_id',
                    'time_spent_hrs']
    student_dtypes = ['int', 'string', 'date',
                    'int', 'string', 'string',
                    'string', 'string', 'string', 'int',
                    'int', 'int', 'int',
                    'float']
    correct_dtype_dict = {key:value for (key,value) in zip(student_columns,student_dtypes)}

    for dtype in current_dtype_list:
        col_name = dtype[0]
        col_dtype = dtype[1]
        try:
            assert col_dtype == correct_dtype_dict[col_name], f'dtype mismatch in column {col_name}'
        except AssertionError as ae:
            logger.exception(ae)
            logger.exception(f'{col_name} should be {correct_dtype_dict[col_name]} but is {col_dtype}')
            raise ae

def test_db_existence():
    con = sqlite3.connect(os.path.join('cademycode_updated.db'))
    cur = con.cursor()
    existing_tables = cur.execute('''SELECT name FROM sqlite_master''').fetchall()
    con.close()
    target_tables = [('student_information',), ('student_details',), ('student_studies',), ('student_contact',), ('course_info',), ('job_info',), ('missing_data_entries',)]
    for table in existing_tables:
        try:
            assert table in target_tables, f'The table {table} was not found in the target database'
        except AssertionError as ae:
            logger.exception(ae)
            logger.exception(f'Error occured while confirming the existence of the target tables')
            return ae

def test_all(student_df, course_df, job_df):
    test_no_course_nulls(course_df)
    test_no_job_nulls(job_df)
    test_no_student_nulls(student_df)
    test_no_negative_values(student_df)
    test_student_df_dtypes(student_df)
    test_course_id_match(student_df, course_df)
    test_job_id_match(student_df, job_df)


def load_new_data(student_df, missing_info_df, course_df, job_df):
    try:
        test_db_existence()
    except AssertionError as ae:
        raise ae
    else: 
        mode = 'overwrite'
        jdbc_url = 'jdbc:sqlite:cademycode_updated.db'
        course_columns = ['career_path_id', 'career_path_name', 'hours_to_complete']
        job_columns = ['job_id', 'job_category', 'avg_salary']
        sorted_columns = ['uuid', 'name', 'dob', 
                    'birth_year', 'sex', 'email_address',
                    'street', 'city', 'state', 'zipcode',
                    'job_id', 'num_course_taken', 'current_career_path_id',
                    'time_spent_hrs']

        student_df = student_df.withColumn('loaded_on_date',f.current_timestamp())

        student_df.select(*['uuid', 'name', 'job_id', 'current_career_path_id', 'loaded_on_date']) \
                        .write.jdbc(url=jdbc_url, mode=mode, table='student_information')
        
        student_df.select(*['uuid','dob','birth_year','sex'])\
                    .write.jdbc(url=jdbc_url, mode=mode, table='student_details')

        student_df.select(*['uuid','num_course_taken','time_spent_hrs'])\
                    .write.jdbc(url=jdbc_url, mode=mode, table='student_studies')

        student_df.select(*['uuid','email_address','street','city','state','zipcode'])\
                    .write.jdbc(url=jdbc_url, mode=mode, table='student_contact')

        course_df= course_df.repartition(1)

        course_df.select(*course_columns)\
                    .write.jdbc(url=jdbc_url, mode=mode, table='course_info')

        job_df.select(*job_columns)\
                    .write.jdbc(url=jdbc_url, mode=mode, table='job_info')

        missing_info_df.select(*sorted_columns)\
                        .withColumn('loaded_on_date',f.current_timestamp()) \
                        .write.jdbc(url=jdbc_url, mode=mode, table='missing_data_entries')

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
    con.close()

    test_all(student_df=student_df,
            course_df=courses_df,
            job_df=jobs_df) 

    load_new_data(student_df, missing_info_df, courses_df, jobs_df)

    spark.stop()
    

if __name__ == "__main__":
    main()