import getpass
import re

import psycopg2

import agenspy.cursor
import agenspy.types

################################################################################
# Graph (class) ################################################################
################################################################################

class Graph(agenspy.cursor.Cursor):
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
        return self.execute('SHOW graph_path;').fetchone()[0]

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

    @property
    def ne(self):
        return self.execute('MATCH ()-[e]->() RETURN count(e);').fetchone()[0]

    @property
    def elabels(self):
        return self.xlabels('e')

    def create_nodes(self, n, labels=None, properties=None):
        if labels:
            labels = labels if isinstance(labels, list) else n*[labels]
            assert n == len(labels), 'List of labels has to have n elemnts.'
        if properties:
            properties = properties if isinstance(properties, list) else n*[properties]
            properties = [str(prob) for prob in properties]
            assert n == len(properties), 'List of properties has to have n elements.'
        if labels and properties:
            argslist = [(label, prob) for label, prob in zip(labels, properties)]
            psycopg2.extras.execute_batch('CREATE (:%s %s)', argslist)
        elif not labels:
            argslist = [(prob,) for prob in properties]
            psycopg2.extras.execute_batch('CREATE (%s)', argslist)
        elif not properties:
            argslist = [(label,) for label in labels]
            psycopg2.extras.execute_batch('CREATE (:%s)', argslist)

    def create_node(self, label=None, properties={}, **kwargs):
        '''
        Args:

            label (str): node label (will be created if it does not exist)
            properties (dict): property dictionary
            kwargs: additional properties as keyword arguments

        Returns:

            agenspy.types.GraphVertex: a agenspy.types.GraphVertex instance corresponding to the created node

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
        return agenspy.GraphVertex(ID, self)

    def create_edge(self, source, relation=None, target=None, properties={}, **kwargs):
        '''
        Args:

            source (agenspy.types.GraphVertex): source
            relation: list of str or str
            target (agenspy.types.GraphVertex): target
            properties (dict): properties
            kwargs: additional properties as keyword arguments

        Returns:

            agenspy.types.GraphEdge: A agenspy.types.GraphEdge instance corresponding to the created edge

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
        cmd.append('('+source._match('s'))
        cmd.append('AND')
        cmd.append(target._match('t')+')')
        cmd.append('CREATE (s)-['+_relation+']->(t)')
        cmd.append('RETURN id(e);')
        self.execute(' '.join(cmd))
        ID = self.fetchone()[0]
        return agenspy.types.GraphEdge(ID, self, source.id, target.id)

    def create_self_loop(self, node, relation=None, properties={}, **kwargs):
        '''
        Args:

            node (agenspy.types.GraphVertex): node on which to create the loop
            relation: list of str or str
            properties (dict): properties
            kwargs: additional properties as keyword arguments

        Returns:

            agenspy.types.GraphEdge: A agenspy.types.GraphEdge instance corresponding to the created edge

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
        return agenspy.types.GraphEdge(ID, self, node.id, node.id)

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

    def subgraph_query(self, query):
        self.execute(query)
        tuples = self.fetchall()
        nodes = list({entity for t in tuples for entity in t if isinstance(entity, agenspy.GraphVertex)})
        edges = list({entity for t in tuples for entity in t if isinstance(entity, agenspy.GraphEdge)})
        return Subgraph(nodes, edges)

    def subgraph(self,
                 source_label=None,
                 source_property_filter=None,
                 source_property_proj=None,
                 edge_label=None,
                 edge_property_filter=None,
                 edge_property_proj=None,
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
        if edge_property_proj is None:
            ret.append('properties(e)')
        else:
            #props = ['e->>'+p for p in edge_properties]
            #ret.append(', '.join(probs))
            ret.append('properties(e)')
        cmd.append(', '.join(ret))
        cmd[-1] += ';'
        self.execute(' '.join(cmd))
        edges = [agenspy.types.GraphEdge(ID=edge[0],
                           graph=self,
                           sid=edge[1],
                           tid=edge[2],
                           label=edge[3],
                           properties=edge[4])
                 for edge in self.fetchall()]
        node_ids = { edge.sid for edge in edges } | { edge.tid for edge in edges }
        cmd = ['MATCH (v) WHERE (SELECT CAST(id(v) AS text) in (']
        cmd.append(', '.join(["'{}'".format(ID) for ID in node_ids]))
        cmd.append(') RETURN')
        cmd.append(', '.join(['id(v)', 'label(v)', 'properties(v)']))
        cmd[-1] += ';'
        self.execute(' '.join(cmd))
        nodes = [agenspy.types.GraphVertex(ID=node[0],
                                           graph=self,
                                           label=node[1],
                                           properties=node[2])
                 for node in self.fetchall()]
        return Subgraph(nodes, edges, normalized=True)

    def to_networkx(self, match=None, where=None):
        pass

    def create_from_networkx(self, G):
      # --------------------- #
        import networkx as nx
      # --------------------- #
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
                           unique_node_attr=None,
                           edge_label_attr=None,
                           edge_label=None,
                           return_subgraph=True,
                           strip_attrs=False,
                           strip_tokens={' ', '/', '-'},
                           copy_graph=False):
        '''

        '''
      # ------------------- #
        import igraph as ig
      # ------------------- #
        # TODO BATCH THIS SOMEHOW

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
        if return_subgraph:
            return Subgraph(nodes, edges, normalized=True)

    @classmethod
    def from_igraph(cls, G, **kwargs):
        graph = cls(**kwargs)
        graph.create_from_igraph(G)
        return graph

    def to_networtkit(self, match=None, where=None):
        pass

    def create_from_networkit(self, G):
      # ---------------------- #
        import networkit as nk
      # ---------------------- #
        pass

    def to_graphtool(self, match=None, where=None):
        pass

    def create_from_graphtool(self, G):
      # ----------------------- #
        import graph_tool as gt
      # ----------------------- #
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


class Subgraph:

    def __init__(self, nodes, edges, normalized=None):
        self._nodes = list(set(nodes))
        self._edges = list(set(edges))
        self._normalized = normalized   # True if all nodes are explicit, None if unknown

    @property
    def nodes(self):
        return self._nodes

    @property
    def cached_node_property_keys(self):
        return {key for node in self.nodes for key in node}

    @property
    def edges(self):
        return self._edges

    @property
    def cached_edge_property_keys(self):
        return {key for edge in self.edges for key in edge}

    def add(entity):
        if isinstance(entity, list):
            for e in entity:
                self.add_one(e)
        else:
            self.add_one(entity)

    def add_one(entity):
        if isinstance(entity, agenspy.GraphVertex):
            self.add_vertex(entity)
        elif isinstance(entity, agenspy.GraphEdge):
            self.add_edge(entity)

    def add_vertex(vertex):
        if entity not in self.nodes:
            self._nodes.append(entity)

    def add_edge(edge):
        if entity not in self.edges:
            self._edges.append(entity)
            # normalize
            if entity.source not in self.nodes:
                self._nodes.append(entity.source)
            if entity.target not in self.nodes:
                self._nodes.append(entity.target)

    @property
    def normalized(self):
        if self._normalized is None:
            return self.is_normalized
        return self._normalized

    @property
    def is_normalized(self):
        return all(edge.source in self.nodes for edge in self.edges) and \
               all(edge.target in self.nodes for edge in self.edges)

    def normalize(self):
        nodes = {edge.source for edge in self.edges}
        nodes |= {edge.target for edge in self.edges}
        self._nodes.extend(list(set(self.nodes) - nodes))
        self._normalized = True

    def __len__(self):
        return len(self.nodes)

    def to_igraph(self,
                  node_properties=[],
                  cached_node_properties=True,
                  expand_node_properties=False,
                  node_label='label',
                  node_property_prefix=None,
                  edge_properties=[],
                  cached_edge_properties=True,
                  expand_edge_properties=False,
                  edge_label='label',
                  edge_property_prefix=None,
                  directed=True):
        '''

        '''
      # ------------------- #
        import igraph as ig
      # ------------------- #
        # prepare
        if not self.normalized:
            self.normalize()
        node_property_prefix = node_property_prefix+'_' if node_property_prefix else ''
        edge_property_prefix = edge_property_prefix+'_' if edge_property_prefix else ''
        # graph
        G = ig.Graph(directed=directed)
        # vertices
        G.add_vertices(len(self))
        G.vs[node_label] = [node.label for node in self.nodes]
        if expand_node_properties:
            if cached_node_properties:
                for prop in self.cached_node_property_keys:
                    G.vs[node_property_prefix+prop] = [node.get(prop) for node in self.nodes]
            else:
                pass
        else:
            G.vs[node_property_prefix+'properties'] = [node.properties(cached_node_properties) for node in self.nodes]
        # edges
        id2index = {node.id: index for index, node in enumerate(self.nodes)}
        G.add_edges([(id2index[e.sid], id2index[e.tid]) for e in self.edges])
        G.es[edge_label] = [edge.label for edge in self.edges]
        if expand_edge_properties:
            if cached_edge_properties:
                for prop in self.cached_edge_property_keys:
                    G.es[edge_property_prefix+prop] = [edge.get(prop) for edge in self.edges]
            else:
                pass
        else:
            G.es[edge_property_prefix+'properties'] = [edge.properties(cached_edge_properties) for edge in self.edges]
        # ------ 
        return G

    def to_networkx(self,
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
      # --------------------- #
        import networkx as nx
      # --------------------- #
        # prepare
        if not self.normalized:
            self.normalize()
        nodes = list(self.nodes)
        edges = list(self.edges)
        node_property_prefix = node_property_prefix+'_' if node_property_prefix else ''
        edge_property_prefix = edge_property_prefix+'_' if edge_property_prefix else ''
        # networkx graph
        G = nx.MultiDiGraph() if directed else nx.MultiGraph()

        # ------
        return G

    def to_networkit(self,
                     directed=True,
                     edge_weight_attr=None,
                     default_weight=1.0):
        '''

        '''
      # ---------------------- #
        import networkit as nk
      # ---------------------- #
        if not self.normalized:
            self.normalize()
        # networkit graph
        weight_flag = False if edge_weight_attr is None and default_weight == 1.0 else True
        G = nk.graph.Graph(len(self.nodes), weight_flag, directed)
        nodeid2index = {node.id: index for index, node in enumerate(self.nodes)}
        # weights
        if edge_weight_attr:
            weights = [edge.get(edge_weight_attr) for edge in self.edges]
            weights = [default_weight if w is None else w for w in weights]
        elif edge_weight_attr is None and default_weight != 1.0:
            weights = len(self.edges) * [default_weight]
        # add edges
        if weight_flag:
            for i, edge in enumerate(self.edges):
                G.addEdge(nodeid2index[edge.sid], nodeid2index[edge.tid], weights[i])
        else:
            for edge in self.edges:
                G.addEdge(nodeid2index[edge.sid], nodeid2index[edge.tid])
        # ------
        return G

    def to_graphtool(self):
      # ----------------------- #
        import graph_tool as gt
      # ----------------------- #
        if not self.normalized:
            self.normalize()
        # graph_tool graph
        G = None

        # ------
        return G

    def graphtool_property(self, *args, **kwargs):
        pass
