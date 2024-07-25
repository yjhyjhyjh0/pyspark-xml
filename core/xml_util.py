from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_xml_dict(spark: SparkSession, xml_path):
    """
    read xml with below format
    <root_tag>
    <table name="table1">
        <col name="col1" type="string"/>
        <col name="col2" type="string"/>
        <col name="col3" type="string"/>
    </table>
    :return: dict of table_name: column string
    Ex: {'table1': 'col1 string, col2 string, col3 string', 'table2': 'col1 string, col2 string, col3 string, col4 string'}
    """
    df = spark.read.format("com.databricks.spark.xml") \
        .option("rowTag", "table") \
        .load(xml_path)

    row_list = df.withColumnRenamed("_name", "table_name") \
        .withColumn("col_name", F.col("col._name")) \
        .withColumn("col_type", F.col("col._type")) \
        .withColumn("table_schema", F.zip_with("col_name", "col_type",
                                               lambda c1, c2: F.concat(c1, F.lit(' '), c2))) \
        .withColumn("table_schema_str", F.concat_ws(", ", F.col("table_schema"))) \
        .select("table_name", "table_schema_str") \
        .collect()

    # table_schema_dict= {str(row.table_name) for row in row_list}
    table_schema_dict = {row["table_name"].lower(): row["table_schema_str"] for row in row_list}
    return table_schema_dict


