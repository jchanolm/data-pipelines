import re


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
        
    def drop_na(self, df, column=None):
        return df.drop_na(subset=[f'{column}'], inplace=True)
