from cassandra.cluster import Cluster


class CassandraMgr:
    """
    Manage orerations with Apache Cassandra.
    """

    def __init__(self, config):
        """
        Constructor.
        :param config: configuration of the cluster of Apache Cassandra -> ip, replicator factor, replication class and
        key space.
        """
        self.ip = config['ip']
        self.replication_factor = config["replication_factor"]
        self.replication_class = config["replication_class"]
        self.key_space = config["key_space"]
        self.cluster = Cluster(self.ip)

    def connect(self):
        """
        Create a connection from the configuration passed in class constructor.
        Creates a Keyspace an returns a session.
        :return: session.
        """

        session = self.cluster.connect()

        cql_create_keyspace = """
            CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = { 'class' : '%s', 'replication_factor' : %s }
        """ % (self.key_space, self.replication_class, self.replication_factor)

        try:
            session.execute(cql_create_keyspace)

        except Exception as e:
            print(e)

        try:
            session.set_keyspace(self.key_space )
        except Exception as e:
            print(e)

        return session

    def disconnect(self, session):
        """
        Finalise the session and cluster shutdown.
        :param session: session
        """
        session.shutdown()
        self.cluster.shutdown()

    @staticmethod
    def create_table(session, table, fields, primary_key):
        """
        Create an Apache Cassandra table.
        :param session: session.
        :param table: table to create.
        :param fields: fields of the table.
        :param primary_key: primary key of the table.
        """

        fields_string = ", ".join(fields)
        query = "CREATE TABLE IF NOT EXISTS %s (%s , PRIMARY KEY %s)" % (table, fields_string, primary_key)

        try:
            session.execute(query)
        except Exception as e:
            print(e)

    @staticmethod
    def insert_cassandra_from_df(session, table, columns_table, df):
        """
        Insert a pandas dataframe into a Cassandra table.
        :param session: session.
        :param table: table where insert rows.
        :param columns_table: columns of the table.
        :param df: pandas dataframe to insert into the table.
        """

        query = CassandraMgr.get_insert_query(table, columns_table)

        for index, row in df.iterrows():
            session.execute(query, (row[x] for x in df.columns))

    @staticmethod
    def select(session, fields, table, filters):
        """
        Make a select to an apache Cassandra table.
        :param session: session.
        :param fields: projection of the select statement
        :param table: table
        :param filters: filters of the WHERE clause.
        :return: list of rows of the request.
        """

        fields_string = ", ".join(fields)

        query = "select %s from %s WHERE %s" % (fields_string, table, filters)
        try:
            rows = session.execute(query)
        except Exception as e:
            print(e)

        return rows

    @staticmethod
    def get_insert_query(table: str, columns):
        """
        Builds an INSERT statement string.
        :param table: table
        :param columns: columns to insert.
        :return: string with INSERT query.
        """
        query = "INSERT INTO %s (%s) " % (table, ", ".join(columns))
        query = query + " VALUES (" + ", ".join(["%s"] * len(columns)) + ") "

        return query

    @staticmethod
    def drop_table(session, table):
        """
        Drop an Apache Cassandra table.
        :param session: session.
        :param table: table to drop.
        """
        query = "drop table %s" % table
        try:
            session.execute(query)
        except Exception as e:
            print(e)
