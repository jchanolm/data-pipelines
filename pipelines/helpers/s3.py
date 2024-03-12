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
    def __init__(self, bucket_name=None, metadata_filename=None, load_bucket_data=False, no_bucket_prefix=False):
        self.s3_client = boto3.client("s3")
        self.s3_resource = boto3.resource("s3")
        self.S3_max_size = 1000000000
        
        self.data = {}
        self.metadata = {}
        self.scraper_data = {}
        
        if bucket_name:
            if no_bucket_prefix:
                self.bucket_name = bucket_name
            else:
                self.bucket_name = os.environ["AWS_BUCKET_PREFIX"] + bucket_name
            self.bucket = self.create_or_get_bucket()
        else:
            logging.error("bucket_name is not defined! If this is voluntary, ignore this message.")
        
        if metadata_filename:
            self.metadata_filename = metadata_filename
            self.metadata = self.read_metadata()
        else:
            logging.error("metadata_filename is not defined! If this is voluntary, ignore this message.")

        self.start_date = None
        self.end_date = None
        self.set_start_end_date()
        
        if load_bucket_data:
            self.scraper_data = {}
            self.load_data()

        self.allow_override = False
        if "ALLOW_OVERRIDE" in os.environ and os.environ["ALLOW_OVERRIDE"] == "1":
            self.allow_override = True
        self.data_filename = "data_{}-{}-{}".format(datetime.now().year, datetime.now().month, datetime.now().day)

    def set_start_end_date(self) -> None:
        "Sets the start and end date from either params, env or metadata"
        if not self.start_date and "INGEST_FROM_DATE" in os.environ and os.environ["INGEST_FROM_DATE"].strip():
            self.start_date = os.environ["INGEST_FROM_DATE"]
        else:
            if "last_date_ingested" in self.metadata:
                self.start_date = self.metadata["last_date_ingested"]
        if not self.end_date and "INGEST_TO_DATE" in os.environ and os.environ["INGEST_TO_DATE"].strip():
            self.end_date = os.environ["INGEST_TO_DATE"]
        # Converting to python datetime object for easy filtering
        if self.start_date:
            self.start_date = datetime.strptime(self.start_date, "%Y-%m-%d-%J-%M")
        if self.end_date:
            self.end_date = datetime.strptime(self.end_date, "%Y-%m-%d-%H-%M")

    def get_size(self, 
                 obj: dict|list, 
                 seen = None) -> int:
        """Recursively finds size of objects"""
        size = sys.getsizeof(obj)
        if seen is None:
            seen = set()
        obj_id = id(obj)
        if obj_id in seen:
            return 0
        # Important mark as seen *before* entering recursion to gracefully handle
        # self-referential objects
        seen.add(obj_id)
        if isinstance(obj, dict):
            size += sum([self.get_size(v, seen) for v in obj.values()])
            size += sum([self.get_size(k, seen) for k in obj.keys()])
        elif hasattr(obj, '__dict__'):
            size += self.get_size(obj.__dict__, seen)
        elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
            size += sum([self.get_size(i, seen) for i in obj])
        return size

    def save_json(self, 
                  filename: str, 
                  data: list|dict) -> None:
        "This will save the data field to the S3 bucket set during initialization. The data must be a JSON compliant python object."
        try:
            content = bytes(json.dumps(data).encode("UTF-8"))
        except Exception as e:
            logging.error("The data does not seem to be JSON compliant.")
            raise e
        try:
            self.s3_client.put_object(Bucket=self.bucket_name, Key=filename, Body=content)
        except Exception as e:
            logging.error("Something went wrong while uploading to S3!")
            raise e

    def save_file(self, 
                  local_path: str, 
                  s3_path: str) -> None:
        "This will save the data file to the S3 bucket set during initialization. The data must be a JSON compliant python object."
        try:
            self.s3_client.upload_file(local_path, self.bucket_name, s3_path)
        except Exception as e:
            logging.error("Something went wrong while uploading to S3!")
            raise e
      
    def save_df_as_csv(self, 
                       df: pd.DataFrame, 
                       file_name: str, 
                       ACL = 'public-read', 
                       max_lines: int = 10000, 
                       max_size: int = 10000000)-> list[str]:
        """
        Function to save a Pandas DataFrame to a CSV file in S3.
        This functions takes care of splitting the dataframe if the resulting CSV is more than 10Mb.
        parameters:
        - df: the dataframe to be saved.
        - file_name: The file name (without .csv at the end).
        - ACL: (Optional) defaults to public-read for neo4J ingestion.
        """
        chunks = [df]
        # Check if the dataframe is bigger than the max allowed size of Neo4J (10Mb)
        if df.memory_usage(index=False).sum() > max_size or len(df) > max_lines:
            chunks = self.split_dataframe(df, chunk_size=max_lines)

        logging.info("Uploading data...")
        urls = []
        for chunk, chunk_id in zip(chunks, range(len(chunks))):
            chunk_name = f"{file_name}--{chunk_id}.csv"
            chunk.to_csv(f"s3://{self.bucket_name}/{chunk_name}", index=False, escapechar='\\')
            self.s3_resource.ObjectAcl(self.bucket_name, chunk_name).put(ACL=ACL)
            location = self.s3_client.get_bucket_location(Bucket=self.bucket_name)["LocationConstraint"]
            urls.append("https://s3-%s.amazonaws.com/%s/%s" % (location, self.bucket_name, f"{file_name}--{chunk_id}.csv"))
        return urls

    def save_json_as_csv(self, 
                         data: list[dict], 
                         file_name: str, 
                         ACL = "public-read", 
                         max_lines: int = 10000, 
                         max_size: int = 10000000) -> list[str]:
        """
        Function to save a python list of dictionaries (json compatible) to a CSV in S3.
        This functions takes care of splitting the array if the resulting CSV is more than 10Mb.
        parameters:
        - data: the data array.
        - file_name: The file name (without .csv at the end)
        - ACL: (Optional) defaults to public-read for neo4J ingestion.
        """
        df = pd.DataFrame.from_dict(data)
        return self.save_df_as_csv(df, file_name, ACL=ACL, max_lines=max_lines, max_size=max_size)

    def save_full_json_as_csv(self, 
                              data: dict|list, 
                              file_name: str, 
                              ACL = "public-read") -> str:
        df = pd.DataFrame.from_dict(data)
        df.to_csv(f"s3://{self.bucket_name}/{file_name}.csv", index=False)
        self.s3_resource.ObjectAcl(self.bucket_name, f"{file_name}.csv").put(ACL=ACL)
        location = self.s3_client.get_bucket_location(Bucket=self.bucket_name)["LocationConstraint"]
        url = "https://s3-%s.amazonaws.com/%s/%s" % (location, self.bucket_name, f"{file_name}.csv")
        return url

    def load_csv(self, file_name: str) -> pd.DataFrame | None:
        """
        Convenience function to retrieve a S3 saved CSV loaded as a pandas dataframe.
        """
        try:
            df = pd.read_csv(f"s3://{self.bucket_name}/{file_name}", lineterminator="\n")
            return df
        except:
            return None

    def split_dataframe(self, df: pd.DataFrame, chunk_size: int = 10000) -> list[pd.DataFrame]:
        "Convenience function to split a dataframe into multiple small dataframes"
        chunks = list()
        num_chunks = len(df) // chunk_size + (1 if len(df) % chunk_size else 0)
        for i in range(num_chunks):
            chunks.append(df[i * chunk_size : (i + 1) * chunk_size])
        return chunks

    def load_json(self, filename: str) -> dict:
        "Retrieves a JSON formated content from the S3 bucket"
        try:
            result = self.s3_client.get_object(Bucket=self.bucket_name, Key=filename)
        except Exception as e:
            logging.error("An error occured while retrieving data from S3!")
            raise e
        data = json.loads(result["Body"].read().decode("UTF-8"))
        return data

    def check_if_file_exists(self, filename: str) -> bool:
        "This checks if the filename to be saved already exists and raises an error if so."
        try:
            boto3.resource("s3").Object(self.bucket_name, filename).load()
        except ClientError as e:
            if e.response["Error"]["Code"] != "404":
                logging.error("Something went wrong while checking if the data file already existed in the bucket!")
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
                location = {"LocationConstraint": os.environ["AWS_DEFAULT_REGION"]}
                self.s3_client.create_bucket(Bucket=self.bucket_name, CreateBucketConfiguration=location)
                self.configure_bucket()
                logging.info(f"Creating bucket: {self.bucket_name}")
            except ClientError as e:
                logging.error(f"An error occured during the creation of the bucket: {self.bucket_name}")
                raise e
        else:
            logging.info(f"Using existing bucket: {self.bucket_name}")
        return boto3.resource("s3").Bucket(self.bucket_name)

    def read_metadata(self) -> dict:
        "Access the S3 bucket to read the metadata and returns a dictionary that corresponds to the saved JSON object"
        logging.info("Loading the metadata from S3 ...")
        if "REINITIALIZE" in os.environ and os.environ["REINITIALIZE"] == "1":
            return {}
        if self.check_if_file_exists(self.metadata_filename):
            return self.load_json(self.metadata_filename)
        else:
            return {}

    def save_metadata(self) -> None:
        "Saves the current metadata to S3"
        logging.info("Saving the metadata to S3 ...")
        self.save_json(self.metadata_filename, self.metadata)

    def save_data(self, chunk_prefix: str = "") -> None:
        """
        Saves the current data to S3. 
        This will take care of chunking the data to less than 5Gb for AWS S3 requirements.
        You can specify a chunk_prefix to add to the filename to avoid name collision.
        """
        logging.info("Saving the results to S3 ...")
        logging.info("Measuring data size...")
        data_size = self.get_size(self.data)
        logging.info(f"Data size: {data_size}")
        
        if data_size > self.S3_max_size:
            n_chunks = math.ceil(data_size / self.S3_max_size)
            logging.info(f"Data is too big: {data_size}, chunking it to {n_chunks} chunks ...")
            len_data = {}
            for key in self.data:
                len_data[key] = math.ceil(len(self.data[key])/n_chunks)
            for i in range(n_chunks):
                data_chunk = {}
                for key in self.data:
                    if isinstance(self.data[key], dict):
                        data_chunk[key] = {}
                        chunk_keys = list(self.data[key].keys())[i*len_data[key]:min((i+1)*len_data[key], len(self.data[key]))]
                        for chunk_key in chunk_keys:
                            data_chunk[key][chunk_key] = self.data[key][chunk_key]
                    else:
                        data_chunk[key] = self.data[key][i*len_data[key]:min((i+1)*len_data[key], len(self.data[key]))]
                # Adjust filename format here
                today = datetime.now().strftime("%Y-%m-%d")
                filename = f"data_{today}_{chunk_prefix}{i}.json"
                if not self.allow_override and self.check_if_file_exists(filename):
                    logging.error("The data file for this day has already been created!")
                    sys.exit(0)
                logging.info(f"Saving chunk {i}...")
                self.save_json(filename, data_chunk)
        else:
            # Adjust filename format here for single chunk case
            today = datetime.now().strftime("%Y-%m-%d")
            filename = f"data_{today}_{chunk_prefix}.json"
            if not self.allow_override and self.check_if_file_exists(filename):
                logging.error("The data file for this day has already been created!")
                sys.exit(0)
            self.save_json(filename, self.data)
            
    def get_datafile_from_s3(self) -> list[str]:
        "Get the list of datafiles in the S3 bucket from the start date to the end date (if defined)"
        logging.info("Collecting data files")
        datafiles = []
        for el in map(lambda x: (x.bucket_name, x.key), self.bucket.objects.all()):
            if "data_" in el[1]:
                datafiles.append(el[1])
        get_date = re.compile("data_([0-9]+-[0-9]+-[0-9]+).*\.json")
        dates = [datetime.strptime(get_date.match(key).group(1), "%Y-%m-%d") for key in datafiles]
        datafiles_to_keep = []
        dates_to_keep = []
        for datafile, date in sorted(zip(datafiles, dates), key=lambda el: el[1]):
            if not self.start_date:
                self.start_date = date
            if date >= self.start_date:
                if self.end_date and date >= self.end_date:
                    break
                datafiles_to_keep.append(datafile)
                dates_to_keep.append(date)
        if len(dates_to_keep) == 0:
            logging.error("No data file found that match the current date range")
            sys.exit(1)
        if not self.end_date:
            self.end_date = max(dates_to_keep)
        logging.info("Datafiles for ingestion: {}".format(",".join(datafiles_to_keep)))
        return datafiles_to_keep

    def get_files_urls_from_s3(self, filter: str) -> list[str]:
        "Get the list of datafiles in the S3 bucket from the start date to the end date (if defined)"
        logging.info("Collecting data files")
        datafiles = []
        for el in map(lambda x: (x.bucket_name, x.key), self.bucket.objects.all()):
            if filter in el[1]:
                datafiles.append(el[1])
        location = self.s3_client.get_bucket_location(Bucket=self.bucket_name)["LocationConstraint"]
        locations = []
        for file_name in datafiles:
            url = "https://s3-%s.amazonaws.com/%s/%s" % (location, self.bucket_name, file_name)
            locations.append(url)
        return locations

    def load_data(self) -> None:
        "Loads the data filtered by date saved in the S3 bucket"
        datafiles_to_keep = self.get_datafile_from_s3()
        logging.info("Datafiles for ingestion: {}".format(",".join(datafiles_to_keep)))
        for datafile in datafiles_to_keep:
            tmp_data = self.load_json(datafile)
            for root_key in tmp_data:
                if root_key not in self.scraper_data:
                    self.scraper_data[root_key] = type(tmp_data[root_key])()
                if type(tmp_data[root_key]) == dict:
                    self.scraper_data[root_key] = dict(self.scraper_data[root_key], **tmp_data[root_key])
                if type(tmp_data[root_key]) == list:
                    self.scraper_data[root_key] += tmp_data[root_key]
        logging.info("Data files loaded")

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
