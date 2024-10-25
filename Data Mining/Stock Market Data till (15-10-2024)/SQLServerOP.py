import mysql.connector
from mysql.connector import Error
from getpass import getpass

class MySQL_Server:
    def __init__(self): # Takes the inputs from the user
        self.user_name = input("Enter MySQL user name:  ")
        self.user_pass = getpass("Enetr MySQL Pass:  ")


    def create_server_connecter(self, user_name: str = "", user_pass: str = "", host_name: str = "localhost"):
        # connects to the MySQL server
        print("Default host name set to be \"localhost\"")
        user_name = self.user_name
        user_pass = self.user_pass
        connertor = None
        try:
            connertion = mysql.connector.connect(
                host = host_name,
                user = user_name,
                passwd = user_pass
            )
            print("Server connection sucess")
        except Error as er:
            print("The Error is ", er)
        return connertion


    def create_database(self, connection, query):
        # Creats the database
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            print("Crested DataBase sucessfully")
        except Error as er:
            print("The Error is ", er)


    def create_db_connection(self, db_name, user_name: str = "", user_pass: str = "", host_name: str = "localhost"):
        # connects to the database
        print("Default host name set to be \"localhost\"")
        user_name = self.user_name
        user_pass = self.user_pass
        connection = None
        try:
            connection = mysql.connector.connect(
                host = host_name,
                user = user_name,
                passwd = user_pass,
                database = db_name
            )
            print(f"connected to DataBase {db_name}")
        except Error as er:
            print("The Error is ", er)
        return connection

    def execute_query(self, connection, query):
        # executes the quarys
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            connection.commit()
        except Error as er:
            print("The Error is ", er)

    def read_query(self, connection, query):
        # execute the SELECT comand and returns the list of outputs
        cursor = connection.cursor()
        result = None
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        except Error as er:
            print("The Error is ", er)