import psycopg2
import logging
import pandas.io.sql as sqlio
from psycopg2.extras import execute_values
from psycopg2.extensions import register_adapter, AsIs
import sys
import numpy as np
import pandas as pd
import sqlalchemy as sa
import traceback


class PgConnector:

    def addapt_numpy_float64(numpy_float64):
        return AsIs(numpy_float64)

    def addapt_numpy_int64(numpy_int64):
        return AsIs(numpy_int64)

    def addapt_numpy_float32(numpy_float32):
        return AsIs(numpy_float32)

    def addapt_numpy_int32(numpy_int32):
        return AsIs(numpy_int32)

    register_adapter(np.float64, addapt_numpy_float64)
    register_adapter(np.int64, addapt_numpy_int64)
    register_adapter(np.float32, addapt_numpy_float32)
    register_adapter(np.int32, addapt_numpy_int32)

    def __init__(self, credentials: dict = {}, **kwargs) -> None:
        self.conn = None
        self.engine = None
        self.credentials = credentials
        self.default_credentials = {
            "PGHOST": "localhost",
            "PGUSER": "postgres",
            "PGPORT": "5432",
            "PGDATABASE": "postgres",
            "PGPASSWORD": "postgres",
        }

    def help(self):
        print(
            """To create a conection first init the object with the following params .... """
        )

    def paser_credentials_to_psycopg(self):

        return f"""user={self.credentials['PGUSER']} 
                    password={self.credentials['PGPASSWORD']} 
                    host={self.credentials['PGHOST']} 
                    port={self.credentials['PGPORT']} 
                    database={self.credentials['PGDATABASE']}"""

    def parse_credentials(self):
        if len(self.credentials.keys()) != 0:
            intersection_condition = set(self.default_credentials.keys()).intersection(
                set(self.credentials.keys())
            )
            if len(intersection_condition) > 0:
                """overrrides default credential with new ones,
                if the user fortget to put one
                dafault values will be used"""
                self.default_credentials = {
                    **self.default_credentials,
                    **self.credentials,
                }

                if len(intersection_condition) < len(self.default_credentials.keys()):
                    logging.warning(
                        "Some values were not provided, Therefore default values are going to be used for connection engine"
                    )
        else:
            self.credentials = {**self.credentials, **self.default_credentials}
            """fill with deafult variables"""

    def create_conn(self, **kwargs):

        if self.credentials is not None:
            try:
                self.conn = psycopg2.connect(self.paser_credentials_to_psycopg())
                self.conn.autocommit = True
                logging.info("connected to postgresql !")
            except (Exception, psycopg2.DatabaseError) as error:
                logging.error(error)
                logging.error("It was not possible to connect to Database")
                logging.error(traceback.format_exc())

        else:
            logging.warning(
                "Credentials was not provider, set credentials atribute of PgConnector"
            )

    def create_sqlalchemy_engine(self, **kwargs):

        if self.credentials is not None:
            try:
                self.engine = sa.create_engine(
                    f"postgresql+psycopg2://{self.credentials['PGUSER']}:{self.credentials['PGPASSWORD']}@{self.credentials['PGHOST']}:{self.credentials['PGPORT']}/{self.credentials['PGDATABASE']}"
                )
                self.engine.begin()
            except:
                logging.error(
                    "Credential params does not meet conditions, please make sure that the following values are available"
                )
                logging.error(traceback.format_exc())
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
                logging.error(traceback.format_exc())
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
            logging.error(traceback.format_exc())
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
            logging.error(traceback.format_exc())
            return False

    def parallel_insertions(self, data, insert_query, **kwargs):
        """set the number of jobs"""
        logging.error("Parrllel insertions is not suported")
        pass

    def insert_loop(self, data, insert_query, **kwargs):
        pgsql_cursor = self.create_curs()
        tpls = []
        if (kwargs["parallel"] is not None) and kwargs["parallel"]:
            self.parallel_insertions(
                {"data": data, "insert_query": insert_query, **kwargs}
            )
        else:
            try:
                execute_values(pgsql_cursor, insert_query, data)
                logging.info("inserted values succes !")
            except:
                for it, tpl in enumerate(data):
                    tpls.append(tpl)
                    if ((it + 1) % 10) == 0:
                        try:
                            execute_values(pgsql_cursor, insert_query, tpls)
                            logging.info(
                                "inserted values succes ! (at except) (chunk of 10)"
                            )
                            tpls = []

                        except:
                            for e in tpls:
                                try:
                                    execute_values(pgsql_cursor, insert_query, e)
                                    logging.info(
                                        "inserted values succes ! (at except) (one value)"
                                    )
                                except (Exception, psycopg2.DatabaseError) as error:
                                    logging.error(e)
                                    logging.error("Element error")
                                    logging.error(error)
                                    logging.error(traceback.format_exc())
                                    pass
                                    # sys.exit(1)
                try:
                    execute_values(pgsql_cursor, insert_query, tpls)
                    logging.info("inserted values succes ! (at except) (rest values)")
                except:
                    for e in tpls:
                        try:
                            execute_values(pgsql_cursor, insert_query, e)
                            logging.info(
                                "inserted values succes ! (at except) (one value)"
                            )
                        except (Exception, psycopg2.DatabaseError) as error:
                            logging.error(e)
                            logging.error("Element error")
                            logging.error(error)
                            logging.error(traceback.format_exc())
                            pass

        pgsql_cursor.close()
        self.conn.commit()
