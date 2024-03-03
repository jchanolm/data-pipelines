from datetime import datetime
import os
from . import S3Utils
from .utils import Utils
from .requests import Requests 

class Base(Requests, S3Utils, Utils):
    def __init__(self, bucket_name, metadata_filename, load_data) -> None:
        self.metadata_filename = metadata_filename  # Initialize metadata_filename here
        self.metadata = {}
        self.runtime = datetime.now()
        self.asOf = f"{self.runtime.year}-{self.runtime.month}-{self.runtime.day}-{self.runtime.minute}"
        self.isAirflow = os.environ.get("IS_AIRFLOW", False)

        if bucket_name:
            S3Utils.__init__(self, bucket_name, metadata_filename, load_data)