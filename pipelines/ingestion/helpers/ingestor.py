from ...helpers import Base

class Ingestor(Base):
    def __init__(self, bucket_name, load_data=True, chain="ethereum"):
        Base.__init__(self, bucket_name=bucket_name, metadata_filename="ingestor_metadata.json", load_data=load_data, chain=chain)

        try:
            self.cyphers
        except:
            raise NotImplementedError("Cyphers have not been instanciated to self.cyphers")

    def run(self):
        "Main function to be called. Every ingestor must implement its own run function!"
        raise NotImplementedError("ERROR: the run function has not been implemented!")

    def save_metadata(self):
        "Saves the current metadata to S3"
        self.metadata["last_date_ingested"] = f"{self.runtime.year}-{self.runtime.month}-{self.runtime.day}"
        self.save_json(self.metadata_filename, self.metadata)
