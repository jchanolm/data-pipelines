import re
import pandas as pd 
import logging

class Utils:
    def __init__(self) -> None:
        pass

    def is_zero_address(self, address):
        try:
            value = int(address, 16)
            if value == 0:
                return True
            return False
        except:
            return False

    def str2bool(self, v):
        if v.lower() in ("yes", "true", "t", "y", "1"):
            return True
        elif v.lower() in ("no", "false", "f", "n", "0"):
            return False
        else:
            raise ValueError("String value not covered.")
        
    def sanitize_text(self, text: str|None) -> str:
        """
        Helper function to sanitize text before injecting it into a DB query. 
        Useful for ingesting body of text from any source.
        """
        problematic_characters = ["`", "'", "^", "~", "\r", "\\", "'", '"', "\n"]
        if text:
            return [i.strip().replace(i, "") for i in problematic_characters]
        else:
            return ""
        
    def extractor_data_to_df(self, extractor_data_key):
        try: 
            if extractor_data_key:
                data = self.extractor_data[extractor_data_key]
                data_cols = data[0].keys()
                df = pd.DataFrame(data, columns=data_cols)
                return df 
        except Exception as e:
                logging.error(f"Error converting extractor data to df: {e}")