from typing import Any

MAX_LENGHT = 30

def load_param(path: str):
    import yaml
    from yaml.loader import SafeLoader

    with open(path, 'r', encoding='utf-8') as file:
        data = yaml.load(file, Loader=SafeLoader)
    return data


def load_type():
    types = {
        int: return_int,
        str: return_str,
        list: return_list,
        dict: return_dict,
        tuple: return_tuple,
        type(None): return_none
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
    return types
            
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

def return_none(values):
    return "[]"

def get_shot_name(values: Any):
    load_type()
    try:
        return types[type(values)](values)
    except KeyError:
        raise KeyError(f'Неподдерживаемый тип данных {type(values)}')
        

types = load_type()  