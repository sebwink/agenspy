import agenspy

if __name__ == '__main__':

    G = agenspy.Graph('label_tree',
                         replace=True,
                         dbname='test')

    G.create_vlabel('lifeform')
    G.create_vlabel('humanoid', inherits='lifeform')
    G.create_vlabel('telepathic', inherits='lifeform')
    G.create_vlabel('human', inherits='humanoid')
    G.create_vlabel('talaxian', inherits='humanoid')
    G.create_vlabel('vulcan', inherits=['humanoid', 'telepathic'])

    tom = G.create_node('human', name='Tom')
    neelix = G.create_node('talaxian', name='Neelix')
    tuvok = G.create_node('vulcan', name='Tuvok')

    G.execute("MATCH (v) RETURN v.name, label(v);")
    print(G.query)
    print(G.fetchall())
    print(G.vlabels)
    G.print_vlabel_inheritance()
    G.commit()
    G.close()
