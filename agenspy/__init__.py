import re
import json

import psycopg2

from agenspy.cursor import Cursor
from agenspy.graph import Graph, Subgraph
from agenspy.types import GraphVertex, GraphEdge

# ----- VERTEX --------------------------------------------------------------- #

_vertex_matcher = re.compile(r'(.+)\[(\d+\.\d+)\](.+)')

def _cast_vertex(val, cur):
    if val is None:
        return None
    vertex_match = _vertex_matcher.match(val)
    if vertex_match is None:
        return ValueError
    label = vertex_match.group(1)
    ID = vertex_match.group(2)
    properties = json.loads(vertex_match.group(3))
    try:
        if isinstance(cur, Graph):
            vertex = GraphVertex(ID, cur, label, properties)
        else:
            vertex = {'id': ID, 'label': label, 'properties': properties}
    except:
        return psycopg2.InterfaceError('Bad vertex representation: %s' % val)
    return vertex

VERTEX = psycopg2.extensions.new_type((7012,), 'VERTEX', _cast_vertex)
psycopg2.extensions.register_type(VERTEX)

# ----- EDGE ----------------------------------------------------------------- #

_edge_matcher = re.compile(r'(.+)\[(\d+\.\d+)\]\[(\d+\.\d+),(\d+\.\d+)\](.+)')

def _cast_edge(val, cur):
    if val is None:
        return None
    edge_match = _edge_matcher.match(val)
    if edge_match is None:
        return ValueError
    label = edge_match.group(1)
    ID = edge_match.group(2)
    sid = edge_match.group(3)
    tid = edge_match.group(4)
    properties = json.loads(edge_match.group(5))
    try:
        if isinstance(cur, Graph):
            edge = GraphEdge(ID, cur, sid, tid, label, properties)
        else:
            edge = {'id': ID, 'sid': sid, 'tid': tid, 'label': label, 'properties': properties}
    except:
        return psycopg2.InterfaceError('Bad edge representation: %s' % val)
    return edge

EDGE = psycopg2.extensions.new_type((7022,), 'EDGE', _cast_edge)
psycopg2.extensions.register_type(EDGE)
