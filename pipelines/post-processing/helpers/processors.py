from ...helpers import Base 

class Processor(Base):
    def __init__(self, bucket_name, load_data=False, chain='ethereum'):

        try:
            self.cyphers
        except:
            raise ValueError("Cyphers have not been instantiated to self.cyphers")
        
    
    def run(self):
        """Each post-processor must implement its own run function"""
        raise NotImplementedError("Error: run function not implemented")