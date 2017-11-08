import igraph as ig

import agenspy.graph

if __name__ == '__main__':
    kegg = agenspy.graph.Graph('kegg',
                                  replace=True,
                                  dbname='test')
    graph= ig.Graph.Read_GraphMLz('graphs/kegg.graphml.gz')
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
