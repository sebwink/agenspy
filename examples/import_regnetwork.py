import sys
import time

import igraph as ig

import agenspy.graph

if __name__ == '__main__':
    species = sys.argv[1]
    regnetwork = agenspy.graph.Graph('regnetwork_'+species,
                                     replace=True,
                                     dbname='test')
    graph = ig.Graph.Read_GraphMLz('graphs/regnetwork_{}.graphml.gz'.format(species))
    print(len(graph.vs))
    print(len(graph.es))
    start = time.time()
    regnetwork.create_from_igraph(graph,
                                  node_label_attr='node_type',
                                  edge_label_attr='edge_type',
                                  strip_attrs=True)
    print('--- time: %s seconds ---' %(time.time()-start))
    print(regnetwork.nv)
    print(regnetwork.ne)
    regnetwork.commit()
    regnetwork.close()
