import psycopg2
import logging
import pandas.io.sql as sqlio
from psycopg2.extras import execute_values
from psycopg2.extensions import register_adapter, AsIs
import sys
import numpy as np
import pandas as pd
import sqlalchemy as sa


class PgConnector:

    # def addapt_numpy_float64(numpy_float64):
    #     return AsIs(numpy_float64)

    # def addapt_numpy_int64(numpy_int64):
    #     return AsIs(numpy_int64)

    # register_adapter(np.float64, addapt_numpy_float64)
    # register_adapter(np.int64, addapt_numpy_int64)

    def __init__(self, credentials: dict = None, **kwargs) -> None:
        self.conn = None
        self.engine = None
        self.credentials = credentials

    def help(self):
        print(
            """To create a conection first init the object with the following params .... """
        )

    def create_conn(self, **kwargs):

        if self.credentials is not None:
            try:
                self.conn = psycopg2.connect(**self.credentials)
                self.conn.autocommit = True
                logging.info("connected to postgresql !")
            except (Exception, psycopg2.DatabaseError) as error:
                logging.error(error)
                logging.error("It was not possible to connect to Database")

        else:
            logging.warning(
                "Credentials was not provider, set credentials atribute of PgConnector"
            )

    def create_sqlalchemy_engine(self, **kwargs):

        if self.credentials is not None:
            try:
                self.engine = sa.create_engine(
                    f"postgresql+psycopg2://{self.credentials['user']}:{self.credentials['password']}@{self.credentials['host']}:{self.credentials['port']}/{self.credentials['database']}"
                )
                self.engine.begin()
            except:
                logging.error(
                    "Credential params does not meet conditions, please make sure that the following values are available"
                )
        else:
            logging.warning(
                "Credentials was not provider, set credentials atribute of PgConnector"
            )

    def create_curs(self):
        return self.conn.cursor()

    def close_conn(self):
        self.conn.close()

    def sqlio_query(self, query="", **kwargs):

        if self.engine is not None:
            try:
                df_ = sqlio.read_sql_query(query, self.engine)
                return df_
            except:
                logging.error("Something goes wrong")
                sys.exit()
        else:
            logging.warning("SQL engine was not initialized")

    def execute_query_fetch(self, query="", **kwargs):
        try:
            cursor = self.create_curs()
            cursor.execute(query)
            data = cursor.fetchall()
            cols = cursor.description
            cursor.close()
            logging.info(f"Data fetched {len(data)} rows ...")
            if len(data) == 1:
                return data[0], cols
            else:
                return data, cols
        except (Exception, psycopg2.DatabaseError) as error:
            logging.error("Something happend")
            logging.error(error)
            return None

    def execute_query_(self, query="", **kwargs):
        try:
            cursor = self.create_curs()
            cursor.execute(query)
            self.conn.commit()
            cursor.close()
            logging.info(f" query executed !")
            return True
        except (Exception, psycopg2.DatabaseError) as error:
            logging.error("Something happend")
            logging.error(error)
            return False

    def insert_loop(self, data, insert_query, **kwargs):
        pgsql_cursor = self.create_curs()
        tpls = []
        try:
            execute_values(pgsql_cursor, insert_query, data)
            logging.info("inserted values succes !")
        except:
            for it, tpl in enumerate(data):
                tpls.append(tpl)
                if ((it + 1) % 10) == 0:
                    try:
                        execute_values(pgsql_cursor, insert_query, tpls)
                        logging.info("inserted values succes ! (at except)")
                        tpls = []

                    except:
                        for e in tpls:
                            try:
                                execute_values(pgsql_cursor, insert_query, e)
                            except (Exception, psycopg2.DatabaseError) as error:
                                logging.error(e)
                                logging.error("Element error")
                                logging.error(error)
                                pass
                                # sys.exit(1)
            try:
                execute_values(pgsql_cursor, insert_query, tpls)

            except (Exception, psycopg2.DatabaseError) as error:
                logging.error(tpls)
                logging.error(error)
                pass
                # sys.exit()

        pgsql_cursor.close()
        self.conn.commit()
