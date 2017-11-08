import agensgraph

if __name__ == '__main__':
    kegg = agensgraph.Graph('kegg', dbname='test')
    print('Number of nodes %s' % kegg.nv)
    print('Number of edges %s' % kegg.ne)
    kegg.execute("MATCH ({symbol: 'TP53'})-[:activation]->(t) RETURN t->>'symbol', t->>'ensembl'")
    print(kegg.query)
    for gene in kegg.fetchall():
        print(gene)
    kegg.execute("MATCH ({symbol: 'TP53'})-[:activation]->(t) RETURN count(t)")
    print(kegg.query)
    print(kegg.fetchone()[0])
    tp53graph = kegg.to_igraph(source_property_filter={'symbol':'TP53'},
                               edge_label='activation',
                               expand_node_properties=True)
    print('Number of TP53 activated genes:', len(tp53graph.es))
    for v in tp53graph.vs:
        print(v.attributes())
    for e in tp53graph.es:
        print(tp53graph.vs[e.source]['symbol'], ' --> ', tp53graph.vs[e.target]['symbol'])
