import logging 
import os 
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level="INFO")
if "LOGLEVEL" in os.environ:
    logging.basicConfig(level=os.environ["LOGLEVEL"])