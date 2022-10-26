import psycopg2

from datetime import datetime

from client.config.db_config import db_credentials


class MainDAO:
    def __init__(self):
        connection_url = "dbname=%s user=%s password=%s port=%s host='%s'" % (db_credentials['dbname'],
                                                                              db_credentials['user'],
                                                                              db_credentials['password'],
                                                                              db_credentials['dbport'],
                                                                              db_credentials['host'])
        #  print("connection url:  ", connection_url)
        self.conn = psycopg2.connect(connection_url)
