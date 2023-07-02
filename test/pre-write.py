import warnings
from datetime import datetime
from typing import Any
from typing import Dict

import pandas as pd
import requests
import urlpath

from pyspark.sql import SparkSession

class Application:
    """This class is an helper to query Spark API and save historical."""
    #def __init__(self, spark_session):
    #    self.web_url = spark_session.sparkContext
    #    self.application_id = self.spark_context.applicationId
    def __init__(self, web_url, application_id, api_applications_link="api/v1/applications") -> None:
        self.web_url = web_url
        self.application_id = application_id
        self.api_applications_link = api_applications_link
        
    def get_esecutirs_info(self) -> pd.DataFrame:
        """Retrieve executors info."""
        url_path = urlpath.URL(
            self.web_url,
            self.api_applications_link,
            self.application_id,
            "executors"
        )
        pdb.set_trace()

def create_application_by_url(url):
    print(url)

#if __name__ == "__main__":
    
