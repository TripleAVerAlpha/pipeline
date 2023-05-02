# Python библиотеки
from typing import List, Dict
from copy import deepcopy

# Устанавливаемые библиотеки
from tqdm.autonotebook import tqdm as progress

# Внутренние зависимости
from pipeline import logger
from pipeline.node import Node

class Pipeline(Node):
    def __init__(self, pipeline: List[Node], name: str, param: Dict = None, output: List = ('Прошлый Выход', )):
        self.param = param
        self.name = name
        self.input = []
        self.__build_ready__ = True
        logger.info(f'Создаю {self}')
        self.__add_pipeline__(pipeline.copy())
        self.output = output
        logger.success(f'Создан {self}')


    def __add_node__(self, node_add: Node):
        def get_name(name):
            all_names = tuple(self.nodes.keys())
            if name in all_names:
                a = 1
                name = name + "_"
                while f'{name}{a}' in all_names:
                    a += 1
                return f'{name}{a}'
            else:
                return name

        node_add.name = get_name(node_add.name)
        self.nodes[node_add.name] = node_add
        self.input = self.input + list(node_add.input)
        self.__build_ready__ = False

    def __add_pipeline__(self, pipeline: List[Node]):
        self.nodes = {}
        for node in pipeline:
            self.__add_node__(deepcopy(node))
        self.input = list(set(self.input))
        if not(self.param is None):
            for param in self.param:
                try: 
                    self.input.remove(param)
                except ValueError:
                    logger.warning(f'{param} не используется в Конвеере')
        self.input = tuple(self.input)
            

    def __call__(self, *args: List, **kwds: Dict) -> Dict:
        if self.param is None:
            data_lake = {}
        else:
            data_lake = deepcopy(self.param)

        for node in progress(self.nodes, desc=f'Выполняю {self.name}'):
            try:
                arg = [data_lake[name_input] for name_input in self.nodes[node].input]
            except KeyError:
                have_not = [name_input if not(name_input in data_lake) else None for name_input in self.nodes[node].input]
                logger.error(f"При вызове Узла {self.nodes[node].name} не хватает {have_not} аргументов")
                logger.error(f"data_lake {tuple(data_lake.keys())}")
                raise KeyError(f"При вызове Узла {self.nodes[node].name} не хватает {have_not} аргументов")
            answer = self.nodes[node](*arg)
            if answer is None:
                for name_output in self.nodes[node].output:
                    data_lake[name_output] = None
            else:
                for name_param in answer:
                    data_lake[name_param] = answer[name_param]
        return {output_name:data_lake[output_name] for output_name in self.output}

    def __build_graphviz__(
            self,  
            dot, 
            node_name: str = None,
            data_lake: Dict = None
        ):
        INPUT_NAME = self.name+'_Вход'
        PARAM_NAME = self.name+'_Параметры'
        OUTPUT_NAME = self.name+'_Выход'

        if self.param is None:
            data_lake = {}
        else:
            data_lake = {i: {'name': PARAM_NAME, 'value': self.param[i]} for i in self.param}

        if len(self.input) != 0:
            data_lake = {**data_lake, **{i: {'name': INPUT_NAME, 'value': ''} for i in self.input}}


        with dot.subgraph(name='cluster_'+self.name) as sub_dot:
            sub_dot.attr(label=self.name if node_name is None else node_name)
            sub_dot.node(name=PARAM_NAME, lable='Параметры', shape='box')
            if len(self.input) != 0:
                sub_dot.node(name=INPUT_NAME, label='Вход', shape='box')

            for node_name in self.nodes.keys():
                if  hasattr(self.nodes[node_name], 'func') and hasattr(self.nodes[node_name].func, '__build_graphviz__'):
                    self.nodes[node_name].func.__build_graphviz__(dot = sub_dot)
                else:
                    sub_dot, data_lake = self.nodes[node_name].__build_graphviz__(
                        node_name=node_name,
                        dot = sub_dot,
                        data_lake=data_lake,
                    )
            sub_dot.node(name=OUTPUT_NAME, label='Выход', shape='box')
            output_edge = {}
            for output_name in self.output:
                output_node_name = data_lake[output_name]['name']
                output_value = data_lake[output_name]['value']

                if not (output_node_name in output_edge):
                    output_edge[output_node_name] = []
                output_edge[output_node_name].append(f'{output_name}[{output_value}]')
        
            for output_node_name in output_edge:
                sub_dot.edge(output_node_name, OUTPUT_NAME, "\n".join(output_edge[output_node_name]))

            self.__build_ready__ = True
            self.dot = sub_dot
        return dot, data_lake

    def show(self, path: str = None):
        if not self.__build_ready__:
            import graphviz
            main_dot = graphviz.Digraph()
            with main_dot.subgraph(name=self.name) as dot:
                self.__build_graphviz__(dot=dot)
            self.dot = main_dot

        if path is None:
            return self.dot
        else:
            with open(path, "w", encoding="utf-8") as file:
                file.write(self.dot.source)