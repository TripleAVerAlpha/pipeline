from typing import Any

MAX_LENGHT = 20
types = None  

def load_type():
    global types
    if types is None:
        types = {
            int: return_int,
            str: return_str,
            list: return_list,
            dict: return_dict,
            tuple: return_tuple
        }

        try:
            import pandas as pd
            types[pd.DataFrame] = return_pd_df
        except ImportError:
            pass

        try:
            import numpy as np
            types[np.ndarray] = return_np_array
        except ImportError:
            pass
            
def return_str(values: str):
    if len(values) > MAX_LENGHT:
        return values[:MAX_LENGHT]
    return values

def return_int(values: int):
    return values

def return_pd_df(values):
    return f"pd.DataFrame {values.shape}"

def return_np_array(values):
    return f"np.array {values.shape}"

def return_list(values):
    return return_str(str(values))

def return_tuple(values):
    return return_str(str(values))

def return_dict(values):
    return return_str(str(values))

def get_shot_name(values: Any):
    load_type()
    try:
        return types[type(values)](values)
    except KeyError:
        raise KeyError(f'Неподдерживаемый тип данных {type(values)}')
        