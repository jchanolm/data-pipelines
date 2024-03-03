from datetime import datetime
import os
import logging
from . import S3Utils
from . import Utils
from . import Requests
from . import Multiprocessing

class Base(Requests, S3Utils, Multiprocessing, Utils):
    def __init__(self, bucket_name, metadata_filename, load_data) -> None:
        self.runtime = datetime.now()
        self.asOf = f"{self.runtime.year}-{self.runtime.month}-{self.runtime.day}"
        self.isAirflow = os.environ.get("IS_AIRFLOW", False)

        Requests.__init__(self)
        if bucket_name:
            if load_data:
                S3Utils.__init__(self, bucket_name, metadata_filename, load_data)
            else:
                S3Utils.__init__(self, bucket_name, metadata_filename)
                logging.info("No data to load")

        Utils.__init__(self)