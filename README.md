# pyspark-xml

# Goal  
pyspark read xml schema, read csv input with schmea, write output parquet  

# Usage 
```  
python  pyspark-xml/core/main.py 
```  

# FAQ
Q1-How to add jar to Pycharm?   
A1-For local debug, copy spark-xml_2.12-0.9.0.jar to your local venv/lib/python3.6/site-packages/pyspark/jars    
A2-For online, could spark-submit --packages com.databricks:spark-xml_2.12-0.9.0   

Q2-How to avoid generating _SUCCESS file?  
```  
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
```

Q3-How to use hdfs api via pyspark?
```
uri_class = sc._gateway.jvm.java.net.URI
path_class = sc._gateway.jvm.org.apache.hadoop.fs.Path
file_system_class = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
config_class = sc._gateway.jvm.org.apache.hadoop.conf.Configuration

fs = file_system_class.get(uri_class(hdfs_path), config_class())
folder_status = fs.listStatus(path_class(hdfs_path))
```