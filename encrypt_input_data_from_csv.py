import hashlib
import json

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


class FailedToReadInputFile(Exception):
    pass


class FailedToWriteOutputFiles(Exception):
    pass


def load_settings(path):
    settings = {}
    with open(path) as setting_file:
        settings = json.load(setting_file)
    return settings


def encrypt_value(val):
    return hashlib.sha256(val.encode()).hexdigest()


def encrypt_input_data():
    sc = SparkContext.getOrCreate()
    sqlContext = SQLContext(sc)

    settings_path = './settings.json'
    settings = load_settings(settings_path)

    file_scheme = settings['file_scheme']
    input_path = settings['input_path']
    output_path = settings['output_path']
    encrypt_columns = settings['encrypt_columns']

    try:
        results_df = sqlContext.read.csv(f'{file_scheme}{input_path}', header=True)
    except Exception as e:
        print(e)
        raise FailedToReadInputFile(e)

    encrypt_value_udf = udf(encrypt_value, StringType())

    for column in encrypt_columns:
        results_df = results_df.withColumn(f'encrypted_{column}', encrypt_value_udf(column))

    results_df = results_df.drop(*encrypt_columns)

    # write output files, find number of executors to calculate partitions first
    partitions = sc._jsc.sc().getExecutorMemoryStatus().size() * 4
    try:
        results_df.coalesce(partitions).write.csv(f'{file_scheme}{output_path}',
                                                  sep=',', mode='overwrite', nullValue='\u0000', emptyValue='\u0000')
    except Exception as e:
        print(e)
        raise FailedToWriteOutputFiles(e)


if __name__ == '__main__':
    encrypt_input_data()
