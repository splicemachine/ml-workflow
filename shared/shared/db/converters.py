from datetime import date, datetime, time
from decimal import Decimal

"""
Convert between Python/Spark/SQL types
"""


class Converters:
    @staticmethod
    def remove_spec_detail(sql_name: str) -> str:
        """
        Remove the length/precision/size etc specification
        from a SQL Type. Example: VARCHAR(2000) -> VARCHAR
        :param sql_name:  sql identifier
        :return: cleaned sql identifier
        """
        return sql_name.split('(')[0] if '(' in sql_name else sql_name

    SQL_TYPES = ['CHAR', 'LONG VARCHAR', 'VARCHAR', 'DATE', 'TIME', 'TIMESTAMP', 'BLOB', 'CLOB', 'TEXT', 'BIGINT',
                 'DECIMAL', 'DOUBLE', 'DOUBLE PRECISION', 'INTEGER', 'NUMERIC', 'REAL', 'SMALLINT', 'TINYINT',
                 'BOOLEAN', 'INT']

    SPARK_DB_CONVERSIONS = {
        'BinaryType': 'BLOB',
        'BooleanType': 'BOOLEAN',
        'ByteType': 'TINYINT',
        'DateType': 'DATE',
        'DoubleType': 'FLOAT',
        'DecimalType': 'DECIMAL',
        'IntegerType': 'INTEGER',
        'LongType': 'BIGINT',
        'ShortType': 'SMALLINT',
        'StringType': 'VARCHAR(20000)',
        'TimestampType': 'TIMESTAMP',
        'UnknownType': 'BLOB',
        'FloatType': 'FLOAT'
    }

    DB_SPARK_CONVERSIONS = {
        'FLOAT': 'FloatType',
        'DOUBLE': 'FloatType',
        'BLOB': 'BinaryType',
        'BIGINT': 'LongType',
        'DATE': 'DateType',
        'INTEGER': 'IntegerType',
        'TIMESTAMP': 'TimestampType',
        'VARCHAR': 'StringType',
        'DECIMAL': 'DecimalType',
        'TINYINT': 'ByteType',
        'BOOLEAN': 'BooleanType',
        'SMALLINT': 'ShortType',
        'REAL': 'DoubleType',
        'NUMERIC': 'DecimalType',
        'DOUBLE PRECISION': 'DoubleType',
        'TEXT': 'StringType',
        'CLOB': 'StringType',
        'LONG VARCHAR': 'StringType',
        'CHAR': 'StringType'
    }

    # Converts native python types to DB types
    PY_DB_CONVERSIONS = {
        str: "VARCHAR",
        int: "INT",
        float: 'DOUBLE',
        datetime: "TIMESTAMP",
        bool: "BOOLEAN",
        date: "DATE",
        time: "TIME",
        Decimal: 'DECIMAL'
    }
