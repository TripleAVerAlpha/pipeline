# Python библиотеки
from typing import List, Dict, Tuple

# Внутренние зависимости
from pipeline import logger
from pipeline.utils import get_shot_name

class Node:
    """
    Класс реализующий Узел конвеера 

    Узел - функция которая имеет именнованные входы и выходы
    """
    def __init__(
            self, 
            func: callable, 
            name: str = None, 
            input: Tuple = None, 
            output: Tuple = None
        ):
        """
        Создание Узла

        Параметры
        ----------
        func: callable
            Функция вызываемая при активации узла
        name: str = None (опционально)
            Название узла, если не заданно, берется __name__.
        input: Tuple = None (опционально)
            Названия для входов, если не заданно, берется __annotations__.
        output: Tuple = None (опционально)
            Названия для выходов, если не заданно, берется ('Прошлый Выход', ).
        """
        self.name = func.__name__ if name is None else name
        self.input = tuple(func.__annotations__.keys()) if input is None else input
        self.func = func
        self.output = ('Прошлый Выход', ) if output is None else output
        self.last_output = {self.output[i]: '' for i in range(len(self.output))}
        logger.info(f'Создаю {self.name}')

    def __call__(self, *args: List, **kwds: Dict) -> Dict:
        """
        Вызов функции

        Возвращает
        ----------
        : Dict
            ответ функции
        """
        logger.info(f'Вызываю {self}')
        answer = self.func(*args, **kwds)
        if type(answer) == tuple:
            if len(answer) != len(self.output):
                raise KeyError(f'Узел {self.name} возвращает больше или меньше параметров, чем переданно названий len(answer)({len(answer)}) != len(self.output)({len(self.output)})')
            self.last_output = {self.output[i]: answer[i] for i in range(len(self.output))}
        else:
            if len(self.output) > 1:
                raise KeyError(f'Узел возвращает один параметр, а переданно названий больше len(answer)({len(answer)}) len(self.output)({len(self.output)})')
            self.last_output = {self.output[0]: answer}
        return self.last_output
    
    def __build_graphviz__(
            self, 
            node_name: str, 
            dot, 
            data_lake: Dict
        ):
        dot.node(node_name)

        input_edge = {}
        for input_name in self.input:
            input_node_name = data_lake[input_name]['name']
            input_value = data_lake[input_name]['value']

            if not (input_node_name in input_edge):
                input_edge[input_node_name] = []
            input_edge[input_node_name].append(f'{input_name}[{input_value}]')
    
        for input_node_name in input_edge:
            dot.edge(input_node_name, node_name, "\n".join(input_edge[input_node_name]))


        for output_name in self.output:
            data_lake[output_name] = {
                'name': node_name,
                'value': get_shot_name(self.last_output[output_name])
            }

        return dot, data_lake

def node(
        name: str = None, 
        input_: List[str] = None, 
        output_: List[str] = None
    ) -> Node:
    """
    Декоратор, для превращения функции в Узел(Node)

    Параметры
    ----------
    name: str = None (опционально)
        Имя узла.
    input_: List[str] = None (опционально)
        Входы узла.
    output_: List[str] = None (опционально)
        Выходы узла.

    Возвращает
    ----------
    : Node
        Функцию обернутую в класс Узла(Node)
    """
    def decorator(func: callable):
        nonlocal name, input_, output_
        def wrapper(*args: List, **kwds: Dict):
            return func(*args, **kwds)
        
        wrapper.__name__ = func.__name__

        annotation = func.__annotations__.copy()

        if "return" in func.__annotations__:
            annotation.pop('return')
        
        if input_ is None:
            input_ = tuple(annotation.keys())

        logger.info(f'Превратил функцию {func.__name__} {input_} '\
                                        f'-> Узел {output_}')
        node_obj = Node(func=wrapper, name=name, input=input_, output=output_)
        logger.success(f'Превратил функцию {func.__name__} в {node_obj.name}')
        return node_obj
    return decorator