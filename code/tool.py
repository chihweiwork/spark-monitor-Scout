import traceback
import sys
import json
import pdb
from functools import wraps
import pandas as pd
import numpy as np
from typing import Dict
from typing import Any
from typing import List
from typing import Iterable

def exception_info(limit=None, file=sys.stderr, chain=True):
    """ 
    Function to print exception.
    :param limit: 
    :param file: output file path
    :param chain: 
    """
    traceback.print_exc(
        limit=None,
        file=file,
        chain=chain
    )

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)

class MonitorData:
    def __init__(self, data: Any) -> None:
        
        self.mode_mapping = {
            'overwrite':'w', 'append':'a'
        }
        self.generator = self.transform2generator(data)

    def transform2generator(self, data) -> Iterable:

        if isinstance(data, pd.DataFrame):
            for _, row in data.iterrows():
                yield row.to_dict()

        elif isinstance(data, dict):
           for x in [data]:
                yield data

        elif isinstance(data, list):
            for item in data:
                yield item

        else:
            print(data)
            raise TypeError(f"Input is unsupported {type(self.data)}")

    def write2text(self, file_name: str, mode: str = "append") -> None:
        
        tf = open(file_name, self.mode_mapping[mode])
        for row in self.generator:
            try:
                # try to write text file
                if isinstance(row, dict): 
                    row = json.dumps(row, cls=NpEncoder)
                else: 
                    row = f"{row}"

                tf.write(f"{row}\n")
            except:
                exception_info()
        tf.close()
        
