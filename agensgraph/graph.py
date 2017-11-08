import re
import getpass

import psycopg2

import agensgraph.cursor
import agensgraph.entity

################################################################################
# Graph (class) ################################################################
################################################################################

class Graph(agensgraph.cursor.Cursor):
    '''

    '''

    def __init__(self,
                 graph_name,
                 authorization=None,
                 cursor_name=None,
                 replace=False,
                 host='127.0.0.1',
                 port='5432',
                 getpass_passwd=False,
                 **kwargs):
        '''

        '''
        if getpass_passwd:
            pass # TODO
        connection = psycopg2.connect(host=host, port=port, **kwargs)
        super().__init__(connection, cursor_name)
        self._name = graph_name
        authorization = kwargs.get('user', getpass.getuser()) if authorization is None else authorization
        if replace:
            self.drop_graph(graph_name, if_exists=True)
        self.create_graph(graph_name, if_not_exists=True, authorization=authorization)
        self._graphid = self._get_graph_id()
        self.execute('SET graph_path = {};'.format(graph_name))

    def _get_graph_id(self):
        self.execute("SELECT nspid FROM pg_catalog.ag_graph WHERE graphname = '{}';"
                     .format(self.name))
        return self.fetchone()[0] + 1

    @property
    def name(self):
        return self._name

    @property
    def graphid(self):
        return self._graphid

    @property
    def graph_path(self):
        self.execute('SHOW graph_path;').fetchone()[0]

    @graph_path.setter
    def graph_path(self, graph_name): pass

    @property
    def nv(self):
        return self.execute('MATCH (v) RETURN count(v);').fetchone()[0]

    def numv(self, label=None, prop={}, where=None):
        label = ':'+label if label else ''
        where = ' WHERE '+where if where else ''
        return self.execute('MATCH (v{} {}){} RETURN count(v);'.format(label, prop, where)).fetchone()[0]

    def xlabels(self, x):
        self.execute("SELECT labname FROM pg_catalog.ag_label WHERE graphid = {} AND labkind = '{}';"
                     .format(self.graphid, x))
        return [label[0] for label in self.fetchall()]

    @property
    def vlabels(self):
        return self.xlabels('v')

    def create_nodetype(self, vtype):
        pass

    @property
    def ne(self):
        return self.execute('MATCH ()-[e]->() RETURN count(e);').fetchone()[0]

    @property
    def elabels(self):
        return self.xlabels('e')

    def create_edgetype(self, etype):
        pass

    def create_node(self, label=None, properties={}, **kwargs):
        '''
        Args:

            label (str): node label (will be created if it does not exist)
            properties (dict): property dictionary
            kwargs: additional properties as keyword arguments

        Returns:

            GraphNode: a GraphNode instance corresponding to the created node

        properties, **kwargs --> properties

        CREATE ([:label] [properties]);

        Technically: CREATE (v[:label] [properties]) RETURN id(v);
        '''
        cmd = ['CREATE (v']
        if label:
            cmd.append(':'+label)
        properties = { **properties, **kwargs }
        if properties:
            cmd.append(str(properties))
        cmd.append(')')
        cmd.append('RETURN id(v);')
        self.execute(' '.join(cmd))
        ID = self.fetchone()[0]
        return GraphNode(ID, self)

    def create_edge(self, source, relation=None, target=None, properties={}, **kwargs):
        '''
        Args:

            source (GraphNode): source
            relation: list of str or str
            target (GraphNode): target
            properties (dict): properties
            kwargs: additional properties as keyword arguments

        Returns:

            GraphEdge: A GraphEdge instance corresponding to the created edge

        ------------------------------------------------------------------------

        properties + **kwargs --> properties
        sid = source._id
        tid = source._id

        MATCH (s),(t)
        WHERE id(s) = CAST(sid as graphid)
        AND   id(t) = CAST(tid as graphid)
        CREATE (s)-[e[:relation] [properties]]->(t)
        RETURN id(e);
        '''
        if target is None:
            return self.create_self_loop(source, relation, properties, **kwargs)
        properties = { **properties, **kwargs }
        _relation = 'e'
        if relation:
            _relation += (':'+relation)
        if properties:
            _relation += (' '+str(properties))
        cmd = ['MATCH (s),(t) WHERE']
        cmd.append(source._match('s'))
        cmd.append('AND')
        cmd.append(target._match('t'))
        cmd.append('CREATE (s)-['+_relation+']->(t)')
        cmd.append('RETURN id(e);')
        self.execute(' '.join(cmd))
        ID = self.fetchone()[0]
        return GraphEdge(ID, self, source.id, target.id)

    def create_self_loop(self, node, relation=None, properties={}, **kwargs):
        '''
        Args:

            node (GraphNode): node on which to create the loop
            relation: list of str or str
            properties (dict): properties
            kwargs: additional properties as keyword arguments

        Returns:

            GraphEdge: A GraphEdge instance corresponding to the created edge

        ------------------------------------------------------------------------

        properties + **kwargs --> properties
        vid = node._id

        MATCH (v)
        WHERE id(v) = CAST(vid as graphid)
        CREATE (v)-[e[:relation] [properties]]->(v)
        RETURN id(e);
        '''
        properties = { **properties, **kwargs }
        _relation = 'e'
        if relation:
            _relation += (':'+relation)
        if properties:
            _relation += (' '+str(properties))
        cmd = ['MATCH (v) WHERE']
        cmd.append(node._match('v'))
        cmd.append('CREATE (v)-['+_relation+']->(v)')
        cmd.append('RETURN id(e);')
        ID = self.execute(' '.join(cmd)).fetchone()[0]
        return GraphEdge(ID, self, node.id, node.id)

    def match_nodes(self, labels, properties):
        pass

    def induced_subgraph(self, entities):
        entities = list(entities)
        if isinstance(entities[0], Node):
            return self.node_induced_subgraph(entities)
        else:
            return self.edge_induced_subgraph(entities)

    def edge_induced_subgraph(self, edges):
        pass

    def node_induced_subgraph(self, nodes):
        pass

    def subgraph(self,
                 source_label=None,
                 source_property_filter=None,
                 source_properties=None,
                 edge_label=None,
                 edge_property_filter=None,
                 edge_properties=None,
                 target_label=None,
                 target_property_filter=None,
                 target_properties=None,
                 where_clause=None,
                 conjunctive=True):
        '''
        MATCH (s[:sl] [sp])->[e[:el] [ep]]->(t[:tl] [tp])
        WHERE where_clause(s,e,t)
        RETURN id(e), id(s), id(t), p1(e), ..., pM(e);

        MATCH (v)
        WHERE id(v) in ( CAST(vid1 as graphid), ..., CAST(vidN as graphid) )
        RETURN id(v), p1(v), ..., pN(v);
        '''

        def add_label_and_property_filter(cmd, label, property_filter):
            if label:
                cmd.append(':'+str(label))
            if property_filter:
                cmd.append(str(property_filter))

        cmd = ['MATCH (s']
        add_label_and_property_filter(cmd, source_label, source_property_filter)
        cmd[-1] += ')-[e'
        add_label_and_property_filter(cmd, edge_label, edge_property_filter)
        cmd[-1] += ']->(t'
        add_label_and_property_filter(cmd, target_label, target_property_filter)
        cmd[-1] += ')'
        if where_clause:
            cmd.append('WHERE')
            if conjunctive:
                cmd.append(self._parse_conjnmf(where_clause))
            else:
                cmd.append(self._parse_disjnmf(where_clause))
        cmd.append('RETURN')
        ret = ['id(e)', 'id(s)', 'id(t)', 'type(e)']
        if edge_properties is None:
            ret.append('properties(e)')
        else:
            #props = ['e->>'+p for p in edge_properties]
            #ret.append(', '.join(probs))
            ret.append('properties(e)')
        cmd.append(', '.join(ret))
        cmd[-1] += ';'
        self.execute(' '.join(cmd))
        edges = [GraphEdge(ID=edge[0],
                           graph=self,
                           sid=edge[1],
                           tid=edge[2],
                           label=edge[3],
                           properties=edge[4])
                 for edge in self.fetchall()]
        node_ids = { edge.sid for edge in edges } | { edge.tid for edge in edges }
        cmd = ['MATCH (v) WHERE id(v) in (']
        cmd.append(', '.join(["CAST('{}' as graphid)".format(ID) for ID in node_ids]))
        cmd.append(') RETURN')
        cmd.append(', '.join(['id(v)', 'label(v)', 'properties(v)']))
        cmd[-1] += ';'
        self.execute(' '.join(cmd))
        nodes = [GraphNode(ID=node[0],
                           graph=self,
                           label=node[1],
                           properties=node[2])
                 for node in self.fetchall()]
        return Subgraph(nodes, edges, normalized=True)

    def to_jsgn(self, match=None, where=None):
        pass

    def create_from_jsgn(self, G):
        pass

    def to_networkx(self, match=None, where=None):
        pass

    def create_from_networkx(self, G):
        import networkx as nx
        pass

    def to_igraph(self,
                  source_label=None,
                  source_property_filter=None,
                  source_properties=None,
                  edge_label=None,
                  edge_property_filter=None,
                  edge_properties=None,
                  target_label=None,
                  target_property_filter=None,
                  target_properties=None,
                  where_clause=None,
                  conjunctive=True,
                  **kwargs):
        '''
        '''
        return self.subgraph(source_label,
                             source_property_filter,
                             source_properties,
                             edge_label,
                             edge_property_filter,
                             edge_properties,
                             target_label,
                             target_property_filter,
                             target_properties,
                             where_clause,
                             conjunctive).to_igraph(**kwargs)

    def create_from_igraph(self, G,
                           node_label_attr=None,
                           node_label=None,
                           edge_label_attr=None,
                           edge_label=None,
                           return_entities=True,
                           strip_attrs=False,
                           strip_tokens={' ', '/'},
                           copy_graph=False):
        '''

        '''
      # ------------------- #
        import igraph as ig
      # ------------------- #
        # TODO BATCH THIS

        def strip_igraph_attributes(entities, tokens):
            regex = '|'.join(['('+token+')' for token in tokens])
            regex = re.compile(regex)
            for attr in entities.attributes():
                stripped_attr = regex.sub('_', attr)
                entities[stripped_attr] = [regex.sub('_', val) if isinstance(val, str) else val for val in entities[attr]]
                if stripped_attr != attr:
                    del entities[attr]
        # make keys and values nice
        if copy_graph:
            G = G.copy()
        if strip_attrs:
            strip_igraph_attributes(G.vs, strip_tokens)
            strip_igraph_attributes(G.es, strip_tokens)
        # nodes
        if node_label_attr:
            nodes = [self.create_node(v[node_label_attr],
                                      v.attributes())
                     for v in G.vs]
        else:
            nodes = [self.create_node(node_label,
                                      v.attributes())
                     for v in G.vs]
        # edges
        index2node = {index: node for index, node in enumerate(nodes)}
        if edge_label_attr:
            edges = [self.create_edge(index2node[e.source],
                                      e[edge_label_attr],
                                      index2node[e.target],
                                      e.attributes())
                     for e in G.es]
        else:
            edges = [self.create_edge(index2node[e.source],
                                      edge_label,
                                      index2node[e.target],
                                      e.attributes())
                     for e in G.es]
        if return_entities:
            return nodes, edges

    @classmethod
    def from_igraph(cls, G, **kwargs):
        graph = cls(**kwargs)
        graph.create_from_igraph(G)
        return graph

    def to_networtkit(self, match=None, where=None):
        pass

    def create_from_networkit(self, G):
        import networkit as nk
        pass

    def to_graphtool(self, match=None, where=None):
        pass

    def create_from_graphtool(self, G):
        import graph_tool as gt
        pass

    def _parse_boolean_exprnmf(self, clause, inner, outer):
        if isinstance(clause, str):
            return clause
        inner = '( {} )'.format(inner)
        outer = ' {} '.format(outer)
        clause = [term if isinstance(term, str) else '('+inner.join(term)+')' for term in clause]
        return outer.join(clause)

    def _parse_conjnmf(self, clause):
        '''
        a --> a
        [a,b,c] --> a AND b AND c
        [a,[b,c],[x,y],z] --> a AND (b OR c) AND (x OR y) AND z
        [[a,b,c],[x,y,z]] --> (a OR b OR c) AND (x OR y OR z)
        '''
        return self._parse_boolean_exprnmf(clause, 'OR', 'AND')

    def _parse_disjnmf(self, clause):
        '''
        a --> a
        [a,b,c] --> a OR b OR c
        [a,[b,c],[x,y],z] --> a OR (b AND c) OR (x AND y) OR z
        [[a,b,c],[x,y,z]] --> (a AND b AND c) OR (x AND y AND z)

        '''
        return self._parse_boolean_exprnmf(clause, 'AND', 'OR')

    # -------------------------------------------------------------------------

    def _get_xlabel_id(self, label_name, x):
        self.execute("SELECT labid FROM pg_catalog.ag_label "+\
                     "WHERE graphid = {} AND labname = '{}' AND labkind = '{}';"
                     .format(self.graphid, label_name, x))
        return self.fetchone()[0]

    def _get_vlabel_id(self, label_name):
        return self._get_xlabel_id(label_name, 'v')

    def _get_elabel_id(self, label_name):
        return self._get_xlabel_id(label_name, 'e')

    def _get_xlabel_y(self, x, y):
        self.execute("SELECT {}, labname FROM pg_catalog.ag_label "
                     .format(y)+\
                     "WHERE graphid = {} and labkind = '{}';"
                     .format(self.graphid, x))
        return dict(self.fetchall())

    def _get_vlabel_ids(self):
        return self._get_xlabel_y(x='v', y='labid')

    def _get_elabel_ids(self):
        return self._get_xlabel_y(x='e', y='labid')

    def _get_vlabel_relids(self):
        return self._get_xlabel_y(x='v', y='relid')

    def _get_elabel_relids(self):
        return self._get_xlabel_y(x='e', y='relid')

    def _revd(self, d):
        return {v: k for k,v in d.items()}

    def _get_xlabel_inheritance(self, x):
        relid2name = self._get_xlabel_y(x, y='relid')
        self.execute("SELECT inhparent, relid "+\
                     "FROM pg_catalog.ag_label AS labels "+\
                     "INNER JOIN pg_catalog.pg_inherits AS inheritance "+\
                     "ON labels.relid = inheritance.inhrelid "+\
                     "AND labels.labkind = '{}' AND labels.graphid = {};"
                     .format(x, self.graphid))
        return [(relid2name[p], relid2name[c]) for p, c in self.fetchall()]

    @property
    def vlabel_inheritance(self):
        return self._get_xlabel_inheritance(x='v')

    @property
    def elabel_inheritance(self):
        return self._get_xlabel_inheritance(x='e')

    def print_xlabel_inheritance(self, x):
        parent2child = self._get_xlabel_inheritance(x)
        for p, c in parent2child:
            print(p, ' --> ', c)

    def print_vlabel_inheritance(self):
        self.print_xlabel_inheritance(x='v')

    def print_elabel_inheritance(self):
        self.print_xlabel_inheritance(x='e')

################################################################################
# GraphEntity (class) ##########################################################
################################################################################

class GraphEntity(dict):
    def __init__(self, ID, graph, label=None, properties=None):
        '''
        A client should never instantiate a GraphEntity herself!
        '''
        super().__init__({} if properties is None else properties)
        self._id = ID
        self._graph = graph
        self._label = label
        #self._property_cache = {} if properties is None else properties


    def __hash__(self):
        return hash(self._id)

    @property
    def cached_keys(self):
        return set(self.keys())

    @property
    def graph(self):
        return self._graph

    @property
    def id(self):
        return self._id

    @property
    def label(self):
        return self.get_label()

    def get_label(self, cache=False):
        raise NotImplementedError

    def properties(self, from_cache=True):
        if from_cache:
            return dict(self)
        return self._properties()

    def execute(self, cmd):
        return self._graph.execute(cmd)

    def fetchall(self):
        return self._graph.fetchall()

    def fetchone(self):
        return self._graph.fetchone()

################################################################################
# GraphNode (class) ############################################################
################################################################################

class GraphNode(GraphEntity):

    def _match(self, x):
        return 'id({}) = CAST(\'{}\' as graphid)'.format(x, self._id)

    @property
    def _match_node_asv(self):
        return 'MATCH (v) WHERE '+self._match('v')

    def __getitem__(self, item):
        if item in self:
            return self[item]
        # --- if not cached
        cmd = [self._match_node_asv]
        cmd.append('RETURN v->>\''+item+'\';')
        self.execute(' '.join(cmd))
        return self.fetchone()[0]

    def get_label(self, cache=False):
        if self._label is None:
            cmd = [self._match_node_asv]
            cmd.append('RETURN label(v);')
            label = self.execute(' '.join(cmd)).fetchone()[0]
            if cache:
                self._label = label
            return label
        return self._label

    def _properties(self):
        pass # TODO

    def neighbors(self, depth=1, incoming=True, outgoing=True):
        pass

    def neighborhood_graph(self, depth=1, incoming=True, outgoing=True):
        pass

################################################################################
# GraphEdge (class) ############################################################
################################################################################

class GraphEdge(GraphEntity):

    def __init__(self, ID, graph, sid, tid, label=None, properties=None):
        super().__init__(ID, graph, label, properties)
        self._sid = sid
        self._tid = tid

    def get_label(self, cache=False):
        return self._label # TODO

    def get_properties(self, cache=False):
        pass # TODO

    @property
    def sid(self):
        return self._sid

    @property
    def tid(self):
        return self._tid

    @property
    def source(self):
        return GraphNode(self._sid,
                         self._graph,
                         self._label,
                         self._property_cache)

    @property
    def target(self):
        return GraphNode(self._tid,
                         self._graph,
                         self._label,
                         self._property_cache)

################################################################################
# Subgraph (class) #############################################################
################################################################################

class NodeSet(set):
    pass

class EdgeSet(set):
    pass

class Subgraph:

    def __init__(self, nodes, edges, normalized=None):
        self.nodes = NodeSet(nodes)
        self.edges = EdgeSet(edges)
        self._normalized = normalized

    @property
    def cached_node_property_keys(self):
        return set().union(*[node.cached_keys for node in self.nodes])

    @property
    def cached_edge_property_keys(self):
        return set().union(*[edge.cached_keys for edge in self.edges])

    @property
    def normalized(self):
        if self._normalized is None:
            return self.is_normalized
        return self._normalized

    @property
    def is_normalized(self):
        pass

    def normalize(self):
        pass

    def __len__(self):
        return len(self.nodes)

    def to_igraph(self,
                  cached_node_properties=True,
                  expand_node_properties=False,
                  node_label='label',
                  node_property_prefix=None,
                  cached_edge_properties=True,
                  expand_edge_properties=False,
                  edge_label='label',
                  edge_property_prefix=None,
                  directed=True):
        '''

        '''
        # ----------------------------- #
        import igraph as ig
        # ----------------------------- #
        # prepare
        if not self.normalized:
            self.normalize()
        nodes = list(self.nodes)
        edges = list(self.edges)
        node_property_prefix = node_property_prefix+'_' if node_property_prefix else ''
        edge_property_prefix = edge_property_prefix+'_' if edge_property_prefix else ''
        # graph
        G = ig.Graph(directed=directed)
        # vertices
        G.add_vertices(len(self))
        G.vs[node_label] = [node.label for node in nodes]
        if expand_node_properties:
            if cached_node_properties:
                for prop in self.cached_node_property_keys:
                    G.vs[node_property_prefix+prop] = [node.get(prop) for node in nodes]
            else:
                pass
        else:
            G.vs[node_property_prefix+'properties'] = [node.properties(cached_node_properties) for node in nodes]
        # edges
        id2index = {node.id: index for index, node in enumerate(nodes)}
        G.add_edges([(id2index[e.sid], id2index[e.tid]) for e in edges])
        G.es[edge_label] = [edge.label for edge in edges]
        if expand_edge_properties:
            if cached_edge_properties:
                for prop in self.cached_edge_property_keys:
                    G.es[edge_property_prefix+prop] = [edge.get(prop) for edge in edges]
            else:
                pass
        else:
            G.es[edge_property_prefix+'properties'] = [edge.properties(cached_edge_properties) for edge in edges]
        # ------ 
        return G

    def to_networkx(self, properties_from_cache=True):
        import networkx as nx
        pass

    def to_networkit(self, properties_from_cache=True):
        import networkit as nk

    def to_graphtool(self, properties_from_cache=True):
        import graph_tool as gt

    def to_jsgn(self, path=None, properties_from_cache=True):
        pass

    def to_graphml(self, path, properties_from_cache=True):
        pass
