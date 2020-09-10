class Connection:
    def __init__(self, host, user, password, port, database):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.database = database

    def get_connection_simple(self):
        return 'DRIVER={MySQL ODBC 8.0 ANSI Driver};User='+ self.user+';Password='+ self.password +';Server='+self.host+';Database='+self.database+';Port='+ self.port.__str__() +';'

    def get_connection_spark(self, input_name):
        database = input_name.split('.')[0]
        table = input_name.split('.')[1]
        s = "jdbc:mysql://" + self.host + ":" + self.port.__str__() + "/" + database
        return 'val dfInput = sqlcontext.read.format("jdbc").option("url", "' + s + '").option("driver", "com.mysql.jdbc.Driver").option("dbtable", " ' + table + '" ).option("user", "'+ self.user +'").option("password", "'+self.password+'").load()'
       