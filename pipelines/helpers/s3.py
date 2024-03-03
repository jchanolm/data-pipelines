import json 
import logging 
import math
import os 
import re 
import sys 
from datetime import datetime 
from tqdm import tqdm 

import boto3 
import pandas as pd 
from botocore.exceptions import ClientError 


class S3Utils:
    def __init__(self, bucket_name=None, metadata_filename='metadata.json', load_bucket_data=False, no_bucket_prefix=False):
        super().__init__()
        self.s3_client = boto3.client("s3")
        self.s3_resource = boto3.resource("s3")
        self.S3_max_size = 1000000000

        self.data = {}
        self.metadata = {}
        self.extractor_data = {}

        if bucket_name:
            if no_bucket_prefix:
                self.bucket_name = bucket_name 
            else:
                self.bucket_name = bucket_name
            logging.info(f"Bucket name is {self.bucket_name}")
            self.bucket = self.create_or_get_bucket()
        else:
             logging.error("bucket name is not defined, if this is voluntary ignore")

        if metadata_filename:
            self.metadata_filename = metadata_filename
            self.metadata = self.read_metadata()
        
        
        self.start_date = None 
        self.end_date = None 
        self.set_start_end_date()

        if load_bucket_data:
            self.extractor_data = {}
            self.load_data()

        self.allow_override = False 
        if "ALLOW_OVERRIDE" in os.environ and os.environ["ALLOW_OVERRIDE"] == "1":
            self.allow_override = True 
        self.data_filename = "data_{}-{}-{}".format(datetime.now().year, datetime.now().month, datetime.now().day, datetime.now().second)

    def set_start_end_date(self) -> None: 
        "Sets the start and end date from either params, env, or metadata"
        if not self.start_date and "INGEST_FROM_DATA" in os.environ and os.environ["INGEST_FROM_DATE"].strip():
            self.start_date = os.environ["INGEST_FROM_DATE"]
        else:
            if "last_date_ingested" in self.metadata:
                self.start_date = self.metadata["last_date_ingested"]
            if not self.end_date and "INGEST_TO_DATE" in os.environ and os.environ["INGESTED_TO_DATE"].strip():
                self.end_date = os.environ['INGEST_TO_DATE']
            ## convert to datetime object for easy filtering
            if self.start_date:
                self.start_data = datetime.strptime(self.start_date, "%Y-%m-%d")
            if self.end_date:
                self.end_date = datetime.strptime(self.end_date, "%Y-%m-%d")
        
    def get_size(self,
                obj: dict|list,
                seen = None) -> int:
        """Recursively finds object size"""
        size = sys.getsizeof(obj)
        if seen is None: 
            seen = set()
        obj_id = id(obj)
        if obj_id in seen:
            return 0
    ## mark as seen before entering recursion to handle gracefully ##
        seen.add(obj_id)
        if isinstance(obj, dict):
            size += sum([self.get_size(v, seen) for v in obj.values()])
            size += sum([self.get_size(k, seen) for k in obj.keys()])
        elif hasattr(obj, '__dict__ '):
            size += sum([self.get_size(i, seen) for i in obj])
        elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
            size += sum([self.get_size(i, seen) for i in obj])

        return size 


    def save_json(self,
                  filename: str,
                  data: list|dict) -> None:
        """save file as json in S3"""
        try:
            content = bytes(json.dumps(data).encode("UTF-8"))
        except Exception as e:
            logging.error("The data is not JSON compliant")
            raise e 
        try:
            self.s3_client.put_object(Bucket=self.bucket_name, Key=filename, Body=content)
        except Exception as e:
            logging.error("S3 file upload went wrong")

    def save_json_as_csv(self,
                         data: list[dict],
                         file_name: str, 
                         ACL = "public-read",
                         max_lines: int = 10000,
                         max_size: int = 10000000):
        "Save list of dictionaries as CSV in S3. Dictionaries must by JSON compliant"
        "Splits array if CSV is more than 10mb"
        " -data: the data array"
        " -file_name: The filename *without .csv at end*"
        " - ACL: defaults to public for Neo4J"
        df = pd.DataFrame.from_dict(data)
        
        return self.save_json_as_csv(df, file_name, ACL=ACL, max_lines=max_lines, max_size=max_size)


    def save_file(self,
                  local_path: str,
                  s3_path: str) -> None:
        "Saves data file to S3 bucket. File must by JSON compliant"
        try:
            self.s3_client.upload_file(local_path, self.bucket_name, s3_path)
        except Exception as e:
            logging.error("S3 file upload went wrong")
            raise e 
        

    def save_df_as_csv(self,
                       df: pd.DataFrame, 
                       file_name: str,
                       ACL = 'public-read',
                       max_lines: int = 10000,
                       max_size: int = 10000000):
        chunks = [df]

        if df.memory_usage(index=False).sum() > max_size or len(df) > max_lines:
            chunks = self.split_dataframe(df, chunk_size = max_lines)
        
        logging.info("Uploading data...")
        urls = []
        for chunk, chunk_id in zip(chunks, range(len(chunks))):
            chunk_name = f"{file_name}-{chunk_id}.csv"
            chunk.to_csv(f"s3://{self.bucket_name}/{chunk_name}", index=False, escapechar='\\')
            self.s3_resource.ObjectAcl(self.bucket_name, chunk_name).put(ACL=ACL)
            """very important -- our AWS DOES NOT HAVE REGIONS!!! REMOVED LOCATION STUFF. IN ORIGINAL GITHUB REPO"""
            urls.append(f"https://{self.bucket_name}.s3.amazonaws.com/{file_name}-{chunk_id}.csv")

        return urls

    def load_csv(self, file_name: str) -> pd.DataFrame | None:
        "Convenience function to retrieve CSV in S3 and load as a Pandas DataFrame"
        try:
            df = pd.read_csv(f"s3://{self.bucket_name}/{file_name}", lineterminator="\n")
            return df
        except Exception as e:
            logging.warning(f"Error loading CSV {e}")

    def split_dataframe(self, df: pd.DataFrame, chunk_size: int = 25000):
        "Convenience function to split a df into smaller dfs"
        chunks = list()
        num_chunks = len(df) // chunk_size + (1 if len(df) % chunk_size else 0)
        for i in range(num_chunks):
            chunks.append(df[i * chunk_size: (i + 1) * chunk_size])
        return chunks 
    
    def load_json(self, filename: str) -> dict:
        "Retrieves JSON formatted content from S3"
        try:
            result = self.s3_client.get_object(Bucket=self.bucket_name, Key=filename)
        except Exception as e:
            logging.error("An error occured while retrieving data from S3!")
            raise e 
        data = json.loads(result['Body'].read().decode('UTF-8'))
        return data 
    
    def check_if_file_exists(self, filename: str) -> bool:
        "Checks if filename exists"
        try:
            boto3.resource('s3').Object(self.bucket_name, filename).load()
        except ClientError as e:
            if e.response['Error']['Code'] != '404':
                logging.error("Something went wrong while checking if filename exists in bucket")
                raise e
            else:
                return False
        else:
            return True
            
    def configure_bucket(self):
        self.s3_client.put_public_access_block(
            Bucket=self.bucket_name,
            PublicAccessBlockConfiguration={
                'BlockPublicAcls': False,
                'IgnorePublicAcls': False,
                'BlockPublicPolicy': False,
                'RestrictPublicBuckets': False
            }
        )
        self.s3_client.put_bucket_ownership_controls(
            Bucket=self.bucket_name,
            OwnershipControls={
                'Rules': [
                    {
                        'ObjectOwnership': 'ObjectWriter'
                    },
                ]
            }
        )


    def create_or_get_bucket(self):
        response = self.s3_client.list_buckets()
        if self.bucket_name not in [el["Name"] for el in response["Buckets"]]:
            try:
                logging.warning("Bucket not found! Creating {}".format(self.bucket_name))
                self.s3_client.create_bucket(Bucket=self.bucket_name)
                self.configure_bucket()
                logging.info(f"Creating bucket: {self.bucket_name}")
            except ClientError as e:
                logging.error(f"An error occured during the creation of the bucket: {self.bucket_name}")
                raise e
        else:
            logging.info(f"Using existing bucket: {self.bucket_name}")
        return boto3.resource("s3").Bucket(self.bucket_name)
                
    def read_metadata(self) -> dict:
        "Read metadata from S3 bucket return dict corresponding to saved JSON object"
        if "REINITIALIZE" in os.environ and os.environ["REINITIALIZE"] == "1":
            return {}
        if self.check_if_file_exists(self.metadata_filename):
            return self.load_json(self.metadata_filename)
        else:
            return {}
        
    def save_data(self, chunk_prefix: str = "") -> None:
        logging.info("Saving results to S3...")
        logging.info("Measuring data size...")
        data_size = self.get_size(self.data)
        logging.info(f"Data size: {data_size}")

        # Get current time in S3-friendly format
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")

        if data_size > self.S3_max_size:
            n_chunks = math.ceil(data_size / self.S3_max_size)
            logging.info(f"Data is too big: {data_size}, chunking to {n_chunks} chunks...")
            
            len_data = {}
            for key in self.data:
                len_data[key] = len(self.data[key])
            
            chunk_sizes = {key: math.ceil(len_data[key] / n_chunks) for key in len_data}
            
            for i in range(n_chunks):
                data_chunk = {}
                for key in self.data:
                    start_idx = i * chunk_sizes[key]
                    end_idx = min((i + 1) * chunk_sizes[key], len_data[key])
                    data_chunk[key] = self.data[key][start_idx:end_idx]

                if chunk_prefix:
                    filename = f"{self.data_filename}_{chunk_prefix}_{current_time}_{i}.json"
                else:
                    filename = f"{self.data_filename}_{current_time}_{i}.json"
                
                if not self.allow_override and self.check_if_file_exists(filename):
                    logging.error("Data file for today was already created!")
                    sys.exit(0)
                
                logging.info(f"Saving chunk {i}...")
                self.save_json(filename, data_chunk)
        
        else:
            filename = f"{self.data_filename}_{current_time}.json"
            self.save_json(filename, self.data)

    # def save_data(self, chunk_prefix: str = "") -> None: 
    #     """Saves data to S3"""
    #     """Chunks data to less than 5GB per AWS S3 requirements"""
    #     """You can specify a chunk_prefix for the filename to avoid name collision"""
    #     logging.info("Saving results to S3...")
    #     logging.info("Measuring data size...")
    #     data_size = self.get_size(self.data)
    #     logging.info(f"Data size: {data_size}")

    #     if data_size > self.S3_max_size:
    #         logging.info(f"Data is too big: {data_size}, chunking to {n_chunks} chunks...")
    #         len_data = {}
    #         for key in self.data:
    #             len_data = {}
    #         for key in self.data:
    #             len_data[key] = math.ceil(len(self.data[key]/n_chunks))
    #         for i in range(n_chunks):
    #             data_chunk = {}
    #             for key in self.data:
    #                 if type(self.data[key]) == dict:
    #                     data_chunk[key] = {}
    #                     chunk_keys = list(self.data[key].keys())[i*len_data[key]:min(i+1)*len_data[key], len(self.data[key])]    
    #                     for chunk in chunk_keys:
    #                         data_chunk[key][chunk_key] = self.data[key][chunk_key]
    #                 else:
    #                     data_chunk[key] = self.data[key][i*len_data[key]:min(i+1) * len_data[key], len(self.data[key])]
    #             if chunk_prefix:

    #                 filename = self.data_filename = f"_{chunk_prefix}-{i}.json"

    #             else:
    #                 filename = self.data_filename + ".json"

    #             if not self.allow_override and self.check_if_file_exists(filename):
    #                 logging.error("Data file for today was already created!")
    #                 sys.exit(0)
    #             logging.info(f"Saving chunk {i}...")
    #             self.save_json(filename, data_chunk)
    #     else:
    #         self.save_json(self.data_filename + f"_{chunk_prefix}].json", self.data)

    def save_metadata(self) -> None:
        "Saves the current metadata to S3"
        logging.info("Saving the metadata to S3 ...")
        self.save_json(self.metadata_filename, self.metadata)


    def get_datafile_from_s3(self) -> list[str]:
        logging.info("Collecting files...")
        datafiles = []

        for el in map(lambda x: (x.bucket_name, x.key), self.bucket.objects.all()):
            if "data_" in el[1]:
                datafiles.append(el[1])

        if not datafiles:
            logging.error("No datafiles found.")
            sys.exit(1)

        get_date = re.compile("data_([0-9]*-[0-9]*-[0-9]*).*")
        dates = [datetime.strptime(get_date.match(key).group(1), "%Y-%m-%d-%h") for key in datafiles]
        
        # Find the maximum date.
        max_date = max(dates)
        
        # Create a list to hold the most recent datafiles.
        most_recent_datafiles = [datafile for datafile, date in zip(datafiles, dates) if date == max_date]
        
        logging.info(f"Most recent datafiles for ingestion: {', '.join(most_recent_datafiles)}")

        return most_recent_datafiles

    def get_files_urls_from_s3(self, filter: str) -> list[str]:
        "Get the list of datafiles in the S3 bucket from the start date to the end date (if defined)"
        logging.info("Collecting data files")
        try:
            datafiles = []
            for el in map(lambda x: (x.bucket_name, x.key), self.bucket.objects.all()):
                if filter in el[1]:
                    datafiles.append(el[1])
            locations = []
            for file_name in datafiles:
                url = "https://s3.amazonaws.com/%s/%s" % (self.bucket_name, file_name)
                locations.append(url)
            return locations
        except Exception as e:
            logging.info("perhaps you didn't want to read in files from s3, word")

    def load_data(self, file_type: str = 'json') -> None:

        try:
            datafiles_to_keep = self.get_datafile_from_s3()

            for datafile in datafiles_to_keep:
                if file_type == 'json':
                    tmp_data = self.load_json(datafile)
                elif file_type == 'csv':
                    tmp_data = self.load_csv(datafile)
                else:
                    logging.warning(f"Unknown file type {file_type}")
                    continue 
                
                if tmp_data is not None:
                    for key, value in tmp_data.items():
                        self.extractor_data[key] = value 
                
                logging.info("Data files loaded")
        except Exception as e:
            logging.info("Data files not loaded. If this is on purpose pass! you're good. you are enough.")
            

    def load_data_iterate(self, nb_files=1) -> list[dict]:
        "Generator function to load the datafiles one file at a time. Returns the content of N datafile at a time, N being the nb_files parameter."
        datafiles_to_keep = self.get_datafile_from_s3()
        counter = 0
        data = {}
        for datafile in datafiles_to_keep:
            logging.info(f"Loading datafile: {datafile}")
            tmp_data = self.load_json(datafile)
            for root_key in tmp_data:
                if root_key not in data:
                    data[root_key] = type(tmp_data[root_key])()
                if type(tmp_data[root_key]) == dict:
                    data[root_key] = dict(data[root_key], **tmp_data[root_key])
                if type(tmp_data[root_key]) == list:
                    data[root_key] += tmp_data[root_key]
            counter += 1
            if counter >= nb_files:
                yield data
                data = {}
                counter = 0

    def clean_test_buckets(self, filter):
        response = self.s3_client.list_buckets()
        buckets = [el["Name"] for el in response["Buckets"]]
        buckets = [bucket for bucket in buckets if filter in bucket]
        for bucket_name in tqdm(buckets):
            logging.info(f"Cleaning up: {bucket_name}")
            bucket = self.s3_resource.Bucket(bucket_name)
            bucket_versioning = self.s3_resource.BucketVersioning(bucket_name)
            if bucket_versioning.status == 'Enabled':
                bucket.object_versions.delete()
            else:
                bucket.objects.all().delete()
            response = bucket.delete()

