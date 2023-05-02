from loguru import logger

logger.remove()

from pipeline import Pipeline
from pipeline.node import node, Node
from pipeline.utils import load_param


@node(
        output_=['arg1', 'arg2']
)
def my_valid(arg1: int, arg2: int) -> str:
    if type(arg1) != type(arg2) != int:
        raise ValueError()
    return arg1, arg2

@node(
       output_=['arg1'] 
)
def my_sum(arg1: int, arg2: int) -> int:
    return arg1 + arg2

@node()
def my_print(arg1: int):
    return arg1

param = load_param('config/test_1.yml')
summator = Pipeline(
    [
        my_valid, 
        my_sum, 
        my_sum
    ], 
    param["name"], 
    param["param"],
    param["output"]
)

param = load_param('config/test_2.yml')
printer = Pipeline(
    [
        summator,
        my_print
    ],
    param["name"]
)
print(printer())
printer.show(path=param["dot_path"])
