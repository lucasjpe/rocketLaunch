from extract import SparkHandler
from pyspark.sql.functions import col, lit
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, LongType

from dataclasses import dataclass
from typing import NoReturn

@dataclass
class SchemaStructure:
	schema = StructType([StructField('id', LongType(), True),
			StructField('image_url', StringType(), True),
			StructField('news_site', StringType(), True),
			StructField('published_at', StringType(), True), 
			StructField('title', StringType(), True),
			StructField('updated_at', StringType(), True), 
			StructField('url', StringType(), True)])

class TransformationJob:
	"""Class's purpose is to do transformations on DataFrames."""
	spark: SparkSession = SparkHandler.create_session()

	def read_data(self) -> DataFrame:
		"""Returns a DataFrame object"""
		df: DataFrame  = self.spark\
			.read\
			.option("inferSchema", "true")\
			.option("header", "true")\
			.format("parquet")\
			.load("data_files/space_flight/part-00000-ad7602fa-ad5a-4185-be00-bc2db798fcaf-c000.snappy.parquet")
		return df

	def transform_data(self) -> NoReturn:
		"""Method does a simple transformation on a DataFrame."""
		df: DataFrame = self.read_data()
		df = df.drop("summary").sort(df.updated_at.desc())
		df.write.parquet("data_files/transformation_one")

	def read_load_data(self, file_to_read) -> DataFrame:
		df = self.spark.read.parquet(file_to_read)
		return df

transform_data = TransformationJob()
# transform_data.transform_data()
