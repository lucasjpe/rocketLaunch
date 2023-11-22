import mysql.connector
from pyspark.sql import SparkSession
from transform import SchemaStructure


db_connection = mysql.connector.connect(
  host="172.16.36.79",
  user="rocket",
  password="rockets",
  database="rocket_launches"
)

jdbc_properties = {
    "url":"jdbc:mysql://172.16.36.79:3306/rocket_launches",
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": "rocket",
    "password": "rockets"
}

cursor = db_connection.cursor()

create_table_rocket_launches = """
CREATE TABLE IF NOT EXISTS rocket_launches  
(
id INT (4) NOT NULL AUTO_INCREMENT,
image_url VARCHAR(200) NOT NULL,
news_site VARCHAR(4) NOT NULL, 
published_at VARCHAR(100) NOT NULL,
title VARCHAR(200),
updated_at VARCHAR(200),
url VARCHAR(200),
PRIMARY KEY (id) 
)
"""

with cursor:
    cursor.execute(create_table_rocket_launches)
    # cursor.commit()

spark = SparkSession.builder.getOrCreate()
path = "/home/lucaspereira/rocketLaunchesProj/rocketLaunchETL/data_files/transformation_one/part-00000-0506beb6-cfe8-4cb0-8578-f4ae77a7c553-c000.snappy.parquet"
df = spark.read.schema(SchemaStructure.schema).format("parquet").load(path)

df.write.jdbc(
    url=jdbc_properties["url"],
    table="rocket_launches",
    mode="append",
    properties=jdbc_properties
)

