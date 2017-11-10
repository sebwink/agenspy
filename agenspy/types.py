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

    def __hash__(self):
        return hash(self._id)

    def _match(self, x):
        return 'id({}) = CAST(\'{}\' as graphid)'.format(x, self._id)

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
    def oid(self):
        return self._id.split('.')[0]

    @property
    def index(self):
        return self._id.split('.')[1]

    @property
    def label(self):
        return self.get_label()

    def get_label(self, cache=False):
        raise NotImplementedError

    def properties(self, from_cache=True):
        if from_cache:
            return dict(self)
        return self._properties()

    def _properties(self):
        raise NotImplementedError

################################################################################
# GraphNode (class) ############################################################
################################################################################

class GraphVertex(GraphEntity):

    @property
    def _match_node_asv(self):
        return 'MATCH (v) WHERE '+self._match('v')

    def get(self, item):
        if item in self:
            return self[item]
        # --- if not cached
        cmd = [self._match_node_asv]
        cmd.append('RETURN v->>\''+item+'\';')
        return self.graph.execute(' '.join(cmd)).fetchone()[0]

    def get_label(self, cache=False):
        if self._label is None:
            cmd = [self._match_node_asv]
            cmd.append('RETURN label(v);')
            label = self.graph.execute(' '.join(cmd)).fetchone()[0]
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

    @property
    def _match_edge_ase(self):
        return 'MATCH ()-[e]->() WHERE '+self._match('e')

    def get_label(self, cache=False):
        if self._label is None:
            cmd = [self._match_edge_ase]
            cmd.append('RETURN label(e);')
            label = self.graph.execute(' '.join(cmd)).fetchone()[0]
            if cache:
                self._label = label
            return label
        return self._label

    def get_properties(self, cache=False):
        if cache:
            return dict(self)
        cmd = [self._match_edge_ase]
        cmd.append('RETURN properties(e);')
        return self.graph.execute(' '.join(cmd)).fetchone()[0]

    def get(self, attr):
        if attr in self:
            return self[attr]
        # --- if not cached
        cmd = [self._match_edge_ase]
        cmd.append('RETURN e->>\''+attr+'\';')
        return self.graph.execute(' '.join(cmd)).fetchone()[0]

    @property
    def sid(self):
        return self._sid

    @property
    def tid(self):
        return self._tid

    @property
    def source(self):
        return GraphVertex(self._sid,
                           self._graph)

    @property
    def target(self):
        return GraphVertex(self._tid,
                           self._graph)


class VertexList(list):
    pass

class EdgeList(list):
    pass
