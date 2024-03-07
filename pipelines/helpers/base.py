from datetime import datetime
import os
from . import S3Utils
from . import Utils
from . import Web3Utils
from . import Requests
from . import Multiprocessing

class Base(Requests, S3Utils, Multiprocessing, Utils, Web3Utils):
    def __init__(self, bucket_name, metadata_filename, load_data, chain) -> None:
        self.runtime = datetime.now()
        self.asOf = f"{self.runtime.year}-{self.runtime.month}-{self.runtime.day}"
        self.isAirflow = os.environ.get("IS_AIRFLOW", False)

        Requests.__init__(self)
        if bucket_name:
            S3Utils.__init__(self, bucket_name, metadata_filename, load_data)
        Multiprocessing.__init__(self)
        Utils.__init__(self)
        Web3Utils.__init__(self, chain=chain)
