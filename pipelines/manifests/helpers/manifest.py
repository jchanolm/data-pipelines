from ...helpers import Base 
from datetime import datetime

class Manifest(Base):
    def __init__(self, bucket_name, load_data=False, chain='ethereum'):
        try:
            self.cyphers
            Base.__init__(self, bucket_name=bucket_name, metadata_filename=None, load_data=load_data, chain=chain)
            self.runtime = datetime.now()
        except:
            raise ValueError("Cyphers have not been instantiated to self.cyphers")

    def run(self):
        """Each manifest must implement its own run function"""
        raise NotImplementedError("Error: run function not implemented")