import inspect

class EntityType:
    
    @classmethod
    def name(cls):
        return cls.__name__

    @classmethod
    def inherits(cls):
        return [bcls.__name__ for bcls in cls.__bases__]

    @classmethod
    def is_a(cls):
        isa = [bcls.__name__  for bcls in inspect.getmro(cls)]
        return [name for name in isa if name != 'object']

    @classmethod
    def base_types(cls):
        isa = cls.is_a()
        clsname = cls.__name__
        return [name for name in isa if name != clsname]

    @classmethod
    def get(cls, graph):
        pass

    @classmethod
    def get_nodes(cls, graph):
        pass

    @classmethod
    def get_edges(cls, graph):
        pass


class NodeType(EntityType):
    @classmethod
    def get(cls, graph):
        return cls.get_nodes(graph)

class EdgeType(EntityType):
    @classmethod
    def get(cls, graph):
        return cls.get_edges(graph)

class Entity:
    pass

class Node(Entity):
    pass

class Edge(Entity):
    pass
