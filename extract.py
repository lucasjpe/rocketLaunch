import json
import requests
from typing import List
from requests import Response
from pyspark.sql import SparkSession

class SparkHandler:

	@classmethod
	def create_session(self):
		return SparkSession.builder.getOrCreate()

class Extract:

	def get_data_from_api(self, page) -> Response:
		"""Extracts data from API by page."""
		URL: str = f"https://api.spaceflightnewsapi.net/v4/reports/?offset={page}"
		response: Response = requests.get(URL)
		print(URL)
		return response.json() if response.status_code == 200 else None

	def get_all_pages(self) -> list:
		"""Retrieves every page from the API."""
		all_pages: List = []
		page: int = 0

		while True:
			response: Response = self.get_data_from_api(page)
			if (response):
				result = response.get("results")
				all_pages.extend(result)

			next_page: dict = response.get("next")

			if (not next_page):
				break

			page += 10

		return all_pages

	def api_data_to_parquet(self) -> None:
		"""Returns data from API into a parquet file @ data_files/."""
		data = json.dumps(self.get_all_pages())
		spark = SparkHandler.create_session()
		df = spark.createDataFrame(data=self.get_all_pages())
		df = df.repartition(1)
		df.printSchema()
		df.write.parquet("data_files/space_flight")

if __name__ == "__main__":
	extractor = Extract()
	extractor.api_data_to_parquet()
