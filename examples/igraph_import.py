import igraph as ig

import agensgraph.graph

if __name__ == '__main__':
    kegg = agensgraph.graph.Graph('kegg',
                                  replace=True,
                                  dbname='test')
    graph= ig.Graph.Read_GraphML('graphs/kegg.graphml')
    print(len(graph.vs))
    print(len(graph.es))
    kegg.create_from_igraph(graph,
                            node_label='gene',
                            edge_label_attr='interaction',
                            strip_attrs=True)
    print(kegg.nv)
    print(kegg.ne)
    kegg.commit()
    kegg.close()
