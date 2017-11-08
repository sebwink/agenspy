import agensgraph 

if __name__ == '__main__':

    G = agensgraph.Graph('network',
                         replace=True,
                         dbname='test')

    print(G.nv)
    print(G.vlabels)
    print(G.ne)
    print(G.elabels)

    G.create_vlabel('person')
    G.create_elabel('knows')

    tom = G.create_node('person', name='Tom')
    summer = G.create_node('person', name='Summer')
    pat = G.create_node('person', name='Pat')
    nikki = G.create_node('person', name='Nikki')
    olive = G.create_node('person', name='Olive')
    todd = G.create_node('person', name='Todd')

    neelix = G.create_node('person', name='Neelix')

    print(tom['name'], neelix['name'])

    print(G.nv)
    print(G.vlabels)
    print(G.ne)
    print(G.elabels)

    G.create_edge(pat, 'knows', nikki, since='7h23m45s')
    G.create_edge(tom, 'knows', summer, since='forever')
    G.create_edge(olive, 'knows', todd, since='yesterday')
    G.create_edge(tom, 'knows', neelix, from_voyager=True)

    print(G.nv)
    print(G.vlabels)
    print(G.ne)
    print(G.elabels)

    G.execute("MATCH (n:person {name: 'Tom'})-[:knows]->(m:person) RETURN n.name AS n, m.name AS m;")
    print(G.query)
    print(G.fetchall())
    
    G.execute("MATCH (n:person {name: 'Tom'})-[:knows]->(m:person) RETURN id(m);")
    print(G.query)
    print(G.fetchall())

 #   print('\nCursor history:\n')
 #   print('\n'.join(G.history))

    g = G.to_igraph(node_label='ag_node_label',
                    edge_property_prefix='ag',
                    expand_edge_properties=True)
    for v in g.vs:
        print(v.index, v)
    for e in g.es:
        print(e.source, e.target, e)

    G.commit()
    G.close()
