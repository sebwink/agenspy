'''
This module provides the Cursor class derived from psycopg2.extensions.cursor.

It allows to access an AgensGraph instance and provides a convenient Python
interface to the AgensGraph DDL (Data Definition Language).

More information on the DDL can be obtained here:

    http://www.agensgraph.com/agensgraph-docs/agensgraph_DDL.html
'''

import functools

import psycopg2
import psycopg2.extensions

################################################################################
# execute (decorator) ##########################################################
################################################################################

def execute(query_builder):
    '''
    Decorator for cursor methods which talk to the database.
    The decorated methods are supposed to just return a string
    with the query/command for the database server.

    Args:

        query_builder: Cursor method returning a database command as str

    Returns:

        A decorated method, logging executed commands, etc.
    '''

    @functools.wraps(query_builder)
    def _execute(self, *args, **kwargs):
        query = query_builder(self, *args, **kwargs) + ';'
        super(Cursor, self).execute(query)
        self.history.append(query)
        if self.verbose:
            print(query)
        return self

    return _execute

################################################################################
# AgensCursor (class) ##########################################################
################################################################################

class Cursor(psycopg2.extensions.cursor):
    history = []

    def __init__(self, conn, name=None, verbose=False):
        '''
        Args:

            conn (psycopg2.connection): a psycopg2 connection to the AgensGraph DB
            name (str): name of cursor
        '''
        super().__init__(conn, name)
        self.verbose = verbose
        self.alter_graph = self.AlterGraph(self)
        self.alter_vlabel = self.AlterXLabel(self, 'V')
        self.alter_elabel = self.AlterXLabel(self, 'E')

    def _sql_string_list(self, l):
        return '('+', '.join(l)+')'

    ############################################################################
    # connect (classmethod) ####################################################
    ############################################################################

    @classmethod
    def connect(cls, name=None, **kwargs):
        '''
        Construct cursor without explicit connection. All kwargs except name
        will be passed to psycopg2.connect.

        Args:

            name (str): name of the cursor

        Returns:

            Cursor: Cursor with connection specified by kwargs

        '''
        connection = psycopg2.connect(**kwargs)
        return cls(connection, name)

    ############################################################################
    # graph_path (property) ####################################################
    ############################################################################

    @property
    def graph_path(self):
        '''
        Represents the 'graph_path' variable.

        Reading the property amounts to the query: SHOW graph_path;
        Setting the value to <name> corresponds to: SET graph_path = <name>;

        (http://www.agensgraph.com/agensgraph-docs/agensgraph_DDL.html#graph)
        '''
        self.execute('SHOW graph_path;')
        return self.fetchone()[0]

    @graph_path.setter
    def graph_path(self, graph_name):
        self.execute('SET graph_path = {};'
                     .format(graph_name))

    ############################################################################
    # create_graph (method) ####################################################
    ############################################################################

    @execute
    def create_graph(self, name, if_not_exists=False, authorization=None):
        '''
        Creates a graph.

        Corresponds to the query:

        CREATE GRAPH [ IF NOT EXISTS ] graph_name [AUTHORIZATION role_name];

        (http://www.agensgraph.com/agensgraph-docs/agensgraph_DDL.html#graph-1)

        Args:

            name (str): name of the graph to be created
            if_not_exists (bool): if True, do nothing if the same name already exists
            authorization (str): the role name of the user who will own the new graph

        Returns:

            Cursor: the cursor on which the method was invoked
        '''
        return self._create_graph(name, if_not_exists, authorization)

    @classmethod
    def _create_graph(cls, name, if_not_exists=False, authorization=None):
        '''
        CREATE GRAPH [ IF NOT EXISTS ] graph_name [AUTHORIZATION role_name];

        http://www.agensgraph.com/agensgraph-docs/agensgraph_DDL.html#graph-1
        '''
        cmd = ['CREATE GRAPH']
        if if_not_exists:
            cmd.append('IF NOT EXISTS')
        cmd.append(name)
        if authorization is not None:
            cmd.append('AUTHORIZATION')
            cmd.append(authorization)
        return ' '.join(cmd)

    ############################################################################
    # alter_graph (method) #####################################################
    ############################################################################

    @execute
    def _alter_graph_rename(self, alter_graph_base, new_name):
        '''
        DO NOT USE DIRECTLY. Given a cursor access via:

        > cursor.alter_graph(graph_name).rename(new_name)

        ALTER GRAPH graph_name RENAME TO new_name;

        http://www.agensgraph.com/agensgraph-docs/agensgraph_DDL.html#graph-1
        '''
        return alter_graph_base+' RENAME TO '+new_name

    @execute
    def _alter_graph_owner_to(self, alter_graph_base, new_owner):
        '''
        DO NOT USE DIRECTLY. Given a cursor access via:

        > cursor.alter_graph(graph_name).owner_to(new_owner)

        ALTER GRAPH graph_name OWNER TO {new_owner | CURRENT_USER | SESSION_USER};

        http://www.agensgraph.com/agensgraph-docs/agensgraph_DDL.html#graph-1
        '''
        return alter_graph_base+' OWNER TO '+new_owner

    class AlterGraph:
        '''
        Convenience class providing access 'ALTER GRAPH' commands, namely
        renaming and changing of ownership.

        Given a cursor c specify the graph to alter via c.alter_graph(graph_name)...

        For example to rename a graph with name 'x' to name 'y' with a given
        cursor:

        > cursor.alter_graph('x').rename('y')

        Chaning the owner of a graph called 'g' to 'user' can be achieved via:

        > cursor.alter_graph('g').owner_to('user')

        http://www.agensgraph.com/agensgraph-docs/agensgraph_DDL.html#graph-1
        '''
        def __init__(self, cursor):
            self.cursor = cursor
            self._alter_graph_base = None
            self._rename = self.cursor._alter_graph_rename
            self._owner_to = self.cursor._alter_graph_owner_to

        def __call__(self, name):
            '''
            Specify the graph to alter.

            http://www.agensgraph.com/agensgraph-docs/agensgraph_DDL.html#graph-1

            Args:

                name (str): name of an existing graph

            '''
            self._alter_graph_base = 'ALTER GRAPH '+name

        def rename(self, new_name):
            '''
            Rename a graph.

            > cursor.alter_graph(graph_name).rename(new_name)

            corresponds to:

                ALTER GRAPH graph_name RENAME TO new_name;

            http://www.agensgraph.com/agensgraph-docs/agensgraph_DDL.html#graph-1

            Args:

                new_name (str): new name of the graph

            Return:

                Cursor
            '''
            return self._rename(self._alter_graph_base, new_name)

        def owner_to(self, new_owner):
            '''
            Change ownership of the graph.

            > cursor.alter_graph(graph_name).owner_to(new_owner)

            corresponds to:

                ALTER GRAPH graph_name OWNER TO new_owner;

            http://www.agensgraph.com/agensgraph-docs/agensgraph_DDL.html#graph-1

            Args:

                new_owner (str): new owner of the graph. Can be
                                 the name of an existing user or
                                 'CURRENT_USER' or'SESSION_USER'
                                 to specify a session-dependent
                                 users

            Returns:

                Cursor
            '''
            return self._owner_to(self._alter_graph_base, new_owner)

    ############################################################################
    # drop_graph (method) ######################################################
    ############################################################################

    @execute
    def drop_graph(self, name, if_exists=False):
        '''
        Drop (delete) a graph.

        Will also drop all object depending on the graph.

        DROP GRAPH [ IF EXISTS ] graph_name CASCADE;

        (http://www.agensgraph.com/agensgraph-docs/agensgraph_DDL.html#graph-1)

        Args:

            name (str): name of the graph
            if_exists (bool): Do not throw an error if the graph does not exist

        Returns:

            Cursor
        '''
        return self._drop_graph(name, if_exists)

    @classmethod
    def _drop_graph(cls, name, if_exists=False):
        '''
        DROP GRAPH [ IF EXISTS ] graph_name CASCADE;
        (http://www.agensgraph.com/agensgraph-docs/agensgraph_DDL.html#graph-1)
        '''
        cmd = ['DROP GRAPH']
        if if_exists:
            cmd.append('IF EXISTS')
        cmd.append(name)
        cmd.append('CASCADE')
        return ' '.join(cmd)

    ############################################################################
    # _create_xlabel (classmethod) #############################################
    ############################################################################

    @classmethod
    def _create_xlabel(cls,
                       x,
                       name,
                       if_not_exists=False,
                       unlogged=False,
                       disable_index=False,
                       inherits=None,
                       storage_parameter=None,
                       tablespace=None):
        '''
        Implementation of CREATE {V,E}LABEL (XLABEL) ... commands.
        See Cursor.create_vlabel and Cursor.create_elabel.

        CREATE [ UNLOGGED ] XLABEL [ IF NOT EXISTS ] label_name [DISABLE INDEX]
               [ INHERITS ( parent_label_name [, ...] ) ]
               [ WITH (storage_parameter)]
               [ TABLESPACE tablespace_name ]

        http://www.agensgraph.com/agensgraph-docs/agensgraph_DDL.html#label

        Args:

            x (str): can be 'V' (VLABEL) or 'E' (ELABEL)
            name (str): name of label
            if_not_exists (bool): If True, do nothing if a label of the same name
                                  already exists (do not throw an error). 
                                  Default: False
            unlogged (bool): Data written to an unlogged label is not recorded
                             to the write-ahead log, which makes unlogged labels
                             considerably faster than logged labels. However,
                             unlogged labels are not crash-safe. Default: False
            disable_index (bool): Create label with invalid index. The invalid
                                  indexes can not be used for searching or
                                  inserting until reindexed. Default: False
            inherits (list or str): label or list of labels from which the label
                                    to be added is a subclass of. Default: None
                                    The optional INHERITS clause specifies a list
                                    of vertex/edge labels. If it is empty, the new
                                    label inherits the initial label.
                                    Use of INHERITS creates a persistent relationship
                                    between the new child label and its parent
                                    label(s). The data of the child label is
                                    included in scans of the parent(s) by default.
            storage_parameter (str): set a storage parameter. Default: None.
            tablespace (str): name of a tablespace the new label will be created
                              in. Default: None

        Returns:

            str: command to be executed by the server
        '''
        cmd = ['CREATE']
        if unlogged:
            cmd.append('UNLOGGED')
        if if_not_exists:
            cmd.append('IF NOT EXISTS')
        cmd.append(x+'LABEL')
        cmd.append(name)
        if disable_index:
            cmd.append('DISABLE INDEX')
        if inherits:
            if isinstance(inherits, list):
                inherits = ', '.join(inherits)
            cmd.append('INHERITS ({})'.format(inherits))
        if storage_parameter:
            cmd.append('WITH ({})'.format(storage_parameter))
        if tablespace:
            cmd.append('TABLESPACE')
            cmd.append(tablespace)
        return ' '.join(cmd)

    ############################################################################
    # _drop_xlabel (classmethod) ###############################################
    ############################################################################

    @classmethod
    def _drop_xlabel(cls, x, name, if_exists=False, cascade=False):
        '''
        Implementation of DROP {V,E}LABEL (XLABEL) ... commands.
        See Cursor.drop_vlabel and Cursor.drop_elabel.

        DROP VLABEL [ IF EXISTS ] label_name [CASCADE]

        http://www.agensgraph.com/agensgraph-docs/agensgraph_DDL.html#label

        Args:

            x (str): can be 'V' (VLABEL) or 'E' (ELABEL)
            name (str): name of an existing label
            if_exists (bool): if True, do not throw an error if the label
                              does not exsit. Default: False
            cascade (bool): Automatically drop objects that depend on the label

        Return:

            str: the command executed by the server
        '''
        cmd = ['DROP']
        cmd.append(x+'LABEL')
        if if_exists:
            cmd.append('IF EXISTS')
        cmd.append(name)
        if cascade:
            cmd.append('CASCADE')
        return ' '.join(cmd)

    ############################################################################
    # alter_xlabel #############################################################
    ############################################################################

    @execute
    def _alter_xlabel_rename(self, alter_xlabel_base, new_name):
        return alter_xlabel_base+' RENAME TO '+new_name

    @execute
    def _alter_xlabel_owner_to(self, alter_xlabel_base, new_owner):
        return alter_xlabel_base+' OWNER TO '+new_owner

    @execute
    def _alter_xlabel_set_storage(self, alter_xlabel_base, mode):
        return alter_xlabel_base+' SET STORAGE '+mode

    @execute
    def _alter_xlabel_set_tablespace(self, alter_xlabel_base, new_tablespace):
        return alter_xlabel_base+' SET TABLESPACE '+new_tablespace

    @execute
    def _alter_xlabel_cluster_on(self, alter_xlabel_base, idxname):
        return alter_xlabel_base+' CLUSTER ON '+idxname

    @execute
    def _alter_xlabel_set_without_cluster(self, alter_xlabel_base):
        return alter_xlabel_base+' SET WITHOUT CLUSTER'

    @execute
    def _alter_xlabel_set_logged(self, alter_xlabel_base):
        return alter_xlabel_base+' SET LOGGED'

    @execute
    def _alter_xlabel_set_unlogged(self, alter_xlabel_base):
        return alter_xlabel_base+' SET UNLOGGED'

    @execute
    def _alter_xlabel_inherit(self, alter_xlabel_base, parent_label):
        return alter_xlabel_base+' INHERIT '+parent_label

    @execute
    def _alter_xlabel_noinherit(self, alter_xlabel_base, parent_label):
        return alter_xlabel_base+' NO INHERIT '+parent_label

    @execute
    def _alter_xlabel_disable_index(self, alter_xlabel_base):
        return alter_xlabel_base+' DISABLE INDEX'

    class AlterXLabel:
        def __init__(self, cursor, X):
            self.cursor = cursor
            self.label = X+'LABEL'
            self._alter_xlabel_base = None
            self._rename = self.cursor._alter_xlabel_rename
            self._owner_to = self.cursor._alter_xlabel_owner_to
            self._set_storage = self.cursor._alter_xlabel_set_storage
            self._set_tablespace = self.cursor._alter_xlabel_set_tablespace
            self._cluster_on = self.cursor._alter_xlabel_cluster_on
            self._set_without_cluster = self.cursor._alter_xlabel_set_without_cluster
            self._set_logged = self.cursor._alter_xlabel_set_logged
            self._set_unlogged = self.cursor._alter_xlabel_set_unlogged
            self._inherit = self.cursor._alter_xlabel_inherit
            self._noinherit = self.cursor._alter_xlabel_noinherit
            self._disable_index = self.cursor._alter_xlabel_disable_index

        def __call__(self, name, if_exists=False):
            self._alter_xlabel_base = 'ALTER '+self.label+' '
            if if_exists:
                self._alter_xlabel_base += 'IF EXISTS '
            self._alter_xlabel_base += name
            return self

        def rename(self, new_name):
            return self._rename(self._alter_xlabel_base, new_name)

        def owner_to(self, new_owner):
            return self._owner_to(self._alter_xlabel_base, new_owner)

        def set_storage(self, mode):
            return self._set_storage(self._alter_xlabel_base, mode)

        def set_tablespace(self, new_tablespace):
            return self._set_tablespace(self._alter_xlabel_base, new_tablespace)

        def cluster_on(self, idxname):
            return self._cluster_on(self._alter_xlabel_base, idxname)

        def set_without_cluster(self):
            return self._set_without_cluster(self._alter_xlabel_base)

        def set_logged(self):
            return self._set_logged(self._alter_xlabel_base)

        def set_unlogged(self):
            return self._set_unlogged(self._alter_xlabel_base)

        def inherit(self, parent_label):
            return self._inherit(self._alter_xlabel_base, parent_label)

        def noinherit(self, parent_label):
            return self._noinherit(self._alter_xlabel_base, parent_label)

        def disable_index(self):
            return self._disable_index(self._alter_xlabel_base)


    ############################################################################
    # create_vlabel (method) ###################################################
    ############################################################################

    @execute
    def create_vlabel(self,
                      name,
                      if_not_exists=False,
                      unlogged=False,
                      disable_index=False,
                      inherits=None,
                      storage_parameter=None,
                      tablespace=None):
        '''
        Create a new node label.

        CREATE [ UNLOGGED ] VLABEL [ IF NOT EXISTS ] label_name [DISABLE INDEX]
               [ INHERITS ( parent_label_name [, ...] ) ]
               [ WITH (storage_parameter)]
               [ TABLESPACE tablespace_name ]

        http://www.agensgraph.com/agensgraph-docs/agensgraph_DDL.html#label

        Args:

            name (str): name of label
            if_not_exists (bool): If True, do nothing if a label of the same name
                                  already exists (do not throw an error). 
                                  Default: False
            unlogged (bool): Data written to an unlogged label is not recorded
                             to the write-ahead log, which makes unlogged labels
                             considerably faster than logged labels. However,
                             unlogged labels are not crash-safe. Default: False
            disable_index (bool): Create label with invalid index. The invalid
                                  indexes can not be used for searching or
                                  inserting until reindexed. Default: False
            inherits (list or str): label or list of labels from which the label
                                    to be added is a subclass of. Default: None
                                    The optional INHERITS clause specifies a list
                                    of vertex/edge labels. If it is empty, the new
                                    label inherits the initial label.
                                    Use of INHERITS creates a persistent relationship
                                    between the new child label and its parent
                                    label(s). The data of the child label is
                                    included in scans of the parent(s) by default.
            storage_parameter (str): set a storage parameter. Default: None.
            tablespace (str): name of a tablespace the new label will be created
                              in. Default: None

        Returns:

            Cursor
        '''
        return self._create_xlabel('V',
                                   name,
                                   if_not_exists,
                                   unlogged,
                                   disable_index,
                                   inherits,
                                   storage_parameter,
                                   tablespace)

    ############################################################################
    # drop_vlabel (method) #####################################################
    ############################################################################

    @execute
    def drop_vlabel(self, name, cascade=False):
        '''
        Drop a node label.

        DROP VLABEL [ IF EXISTS ] label_name [CASCADE]

        http://www.agensgraph.com/agensgraph-docs/agensgraph_DDL.html#label

        Args:

            name (str): name of an existing label
            if_exists (bool): if True, do not throw an error if the label
                              does not exsit. Default: False
            cascade (bool): Automatically drop objects that depend on the label

        Return:

            Cursor
        '''
        return self._drop_xlabel('V', name, cascade)

    ############################################################################
    # create_elabel (method) ###################################################
    ############################################################################

    @execute
    def create_elabel(self,
                      name,
                      if_not_exists=False,
                      unlogged=False,
                      disable_index=False,
                      inherits=None,
                      storage_parameter=None,
                      tablespace=None):
        '''
        Create a new edge label.

        CREATE [ UNLOGGED ] ELABEL [ IF NOT EXISTS ] label_name [DISABLE INDEX]
               [ INHERITS ( parent_label_name [, ...] ) ]
               [ WITH (storage_parameter)]
               [ TABLESPACE tablespace_name ]

        http://www.agensgraph.com/agensgraph-docs/agensgraph_DDL.html#label

        Args:

            name (str): name of label
            if_not_exists (bool): If True, do nothing if a label of the same name
                                  already exists (do not throw an error). 
                                  Default: False
            unlogged (bool): Data written to an unlogged label is not recorded
                             to the write-ahead log, which makes unlogged labels
                             considerably faster than logged labels. However,
                             unlogged labels are not crash-safe. Default: False
            disable_index (bool): Create label with invalid index. The invalid
                                  indexes can not be used for searching or
                                  inserting until reindexed. Default: False
            inherits (list or str): label or list of labels from which the label
                                    to be added is a subclass of. Default: None
                                    The optional INHERITS clause specifies a list
                                    of vertex/edge labels. If it is empty, the new
                                    label inherits the initial label.
                                    Use of INHERITS creates a persistent relationship
                                    between the new child label and its parent
                                    label(s). The data of the child label is
                                    included in scans of the parent(s) by default.
            storage_parameter (str): set a storage parameter. Default: None.
            tablespace (str): name of a tablespace the new label will be created
                              in. Default: None

        Returns:

            Cursor
        '''
        return self._create_xlabel('E',
                                   name,
                                   if_not_exists,
                                   unlogged,
                                   disable_index,
                                   inherits,
                                   storage_parameter,
                                   tablespace)

    ############################################################################
    # drop_elabel (method) #####################################################
    ############################################################################

    @execute
    def drop_elabel(self, name, cascade=False):
        '''
        Drop an edge label.

        DROP ELABEL [ IF EXISTS ] label_name [CASCADE]

        http://www.agensgraph.com/agensgraph-docs/agensgraph_DDL.html#label

        Args:

            name (str): name of an existing label
            if_exists (bool): if True, do not throw an error if the label
                              does not exsit. Default: False
            cascade (bool): Automatically drop objects that depend on the label

        Return:

            Cursor
        '''
        return self._drop_xlabel('E', name, cascade)

    ############################################################################
    # create_property_index (method) ###########################################
    ############################################################################

    @execute
    def create_property_index(self,
                              label_name,
                              attr_expr,
                              expr,
                              unique=False,
                              concurrently=False,
                              index_name=False,
                              if_not_exists=False,
                              method=None,
                              collation=None,
                              opclass=None,
                              descending=False,
                              nulls='LAST',
                              tablespace=None,
                              where=None,
                              storage_parameters=None):
        '''

        '''
        return self._create_property_index(label_name,
                                           attr_expr,
                                           expr,
                                           unique,
                                           concurrently,
                                           index_name,
                                           if_not_exists,
                                           method,
                                           collation,
                                           opclass,
                                           descending,
                                           nulls,
                                           tablespace,
                                           where,
                                           storage_parameters)

    @classmethod
    def _create_property_index(cls,
                               label_name,
                               attr_expr,
                               expr=None,
                               unique=False,
                               concurrently=False,
                               index_name=False,
                               if_not_exists=False,
                               method=None,
                               collation=None,
                               opclass=None,
                               descending=False,
                               nulls=None,
                               tablespace=None,
                               where=None,
                               storage_parameters=None):
        cmd = ['CREATE']
        if unique:
            cmd.append('UNIQUE')
        cmd.append('PROPERTY INDEX')
        if concurrently:
            cmd.append('CONCURRENTLY')
        if index_name:
            if if_not_exists:
                cmd.append('IF NOT EXISTS')
            cmd.append(index_name)
        cmd.append('ON')
        cmd.append(label_name)
        if method:
            cmd.append('USING')
            cmd.append(method)
        if expr is None:
            cmd.append(attr_expr)
        else:
            cmd.append('('+expr+')')
            if collation:
                cmd.append('COLLATE')
                cmd.append(collation)
            if opclass:
                cmd.append(opclass)
            if descending:
                cmd.append('DESC')
            else:
                cmd.append('ASC')
            if not nulls:
                if not descending:
                    cmd.append('NULLS')
                    cmd.append('LAST')
            else:
                cmd.append('NULLS')
                cmd.append(nulls)
            if storage_parameters:
                cmd.append('WITH')
                params = []
                for param in storage_parameters:
                    params.append(param+' = '+str(storage_parameters[param]))
                params = ', '.join(params)
                cmd.append('('+params+')')
            if tablespace:
                cmd.append('TABLESPACE')
                cmd.append(tablespace)
            if where:
                cmd.append('WHERE')
                cmd.append(where)
            return ' '.join(cmd)


    ############################################################################
    # create_constraint (method) ###############################################
    ############################################################################

    @execute
    def create_unique_constraint(self, constraint_name,
                                       label_name,
                                       field_expr):
        return self._create_unique_constraint(constraint_name,
                                              label_name,
                                              field_expr)

    @classmethod
    def _create_unique_constraint(self, constraint_name,
                                        label_name,
                                        field_expr):
        return ' '.join(['CREATE CONSTRAINT',
                         constraint_name,
                         'ON',
                         label_name,
                         'ASSERT',
                         field_expr,
                         'IS UNIQUE'])

    @execute
    def create_check_constraint(self, constraint_name,
                                      label_name,
                                      check_expr):
        return self._create_check_constraint(constraint_name,
                                             label_name,
                                             check_expr)

    @classmethod
    def _create_check_constraint(self, constraint_name,
                                       label_name,
                                       field_expr):
        return ' '.join(['CREATE CONSTRAINT',
                         constraint_name,
                         'ON',
                         label_name,
                         'ASSERT',
                         check_expr])

    ############################################################################
    # create (method) ##########################################################
    ############################################################################

    @execute
    def create(self, source, elabel, target):
        return self._create(source, elabel, target)

    @classmethod
    def _create(cls, source, elabel, target):
        cmd = ['CREATE ']
        cmd.append(source.cypher())
        cmd.append('-[:'+elabel+']->')
        cmd.append(target.cypher())
        return ''.join(cmd)

    ############################################################################
    # execute (method) #########################################################
    ############################################################################

    def execute(self, cmd):
        if isinstance(cmd, list):
            for c in cmd:
                super().execute(c)
            history.extend(cmd)
        else:
            super().execute(cmd)
            self.history.append(cmd)
        return self

    ############################################################################
    # close (method) ###########################################################
    ############################################################################

    def close(self, close_connection=False):
        super().close()
        if close_connection and self.connection:
            self.connection.close()

    ############################################################################
    # commit (method) ##########################################################
    ############################################################################

    def commit(self):
        if self.connection:
            self.connection.commit()
        return self

    ############################################################################

################################################################################
