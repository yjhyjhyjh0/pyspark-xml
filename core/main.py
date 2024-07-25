import os

from pyspark.sql import SparkSession
import xml_util


def init_spark():
    spark = SparkSession \
        .builder \
        .appName("pyspark-xml") \
        .master("local[1]") \
        .getOrCreate()
    return spark


def read_csv(spark: SparkSession, csv_path, schema, delimiter="|"):
    # Could use df.asSchema(new_schema) at spark 3.4.0 to handle multiple schema
    # https://issues.apache.org/jira/browse/SPARK-38904
    # Otherwise can write to temp folder partition by table_name
    df = spark.read.csv(csv_path, sep=delimiter, schema=schema)
    return df


def write_output(df, output_path, table_name):
    df.show(5, False)
    output_path = output_path + "/" + table_name
    df.write.option("mapreduce.fileoutputcommitter.marksuccessfuljobs","false")\
        .parquet(output_path)
#     dataframe.coalesce(1) //So just a single part- file will be created
# .write.mode(SaveMode.Overwrite)
# .option("mapreduce.fileoutputcommitter.marksuccessfuljobs","false") //Avoid creating of crc files
# .option("header","true") //Write the header
# .csv("csvFullPath")



def main():
    print("[INFO] main start")
    input_dir = os.path.dirname(os.path.abspath(__file__))
    input_path = input_dir + "/input"
    output_path = input_dir + "/output"
    xml_path = input_dir + "/schema"
    print("[INFO] input_dir : {}".format(input_dir))
    print("[INFO] input_path : {}".format(input_path))
    print("[INFO] output_path : {}".format(output_path))
    print("[INFO] xml_path : {}".format(xml_path))

    spark = init_spark()
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")


    table_schema_dict = xml_util.get_xml_dict(spark, xml_path)
    table_name = "table1"

    df = read_csv(spark, input_path, table_schema_dict.get(table_name))

    write_output(df, output_path, table_name)
    print("[INFO] main end")


if __name__ == '__main__':
    main()
