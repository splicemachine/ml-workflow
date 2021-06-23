from sqlalchemy import and_, func
from sqlalchemy import inspect as peer_into_splice_db
from sqlalchemy.orm import Session

from shared.db.sql import SQL


class DatabaseFunctions:
    """
    Helper functions for the database
    """

    @staticmethod
    def table_exists(schema_name: str, table_name: str, engine) -> bool:
        """
        Check whether or not a given table exists

        :param schema_name: schema name
        :param table_name: the table name
        :param engine: the SQLAlchemy Engine
        :return: whether exists or not
        """
        inspector = peer_into_splice_db(engine)
        return table_name.lower() in [value.lower() for value in inspector.get_table_names(schema=schema_name)]

    @staticmethod
    def trigger_exists(schema_name: str, trigger_name: str, db) -> bool:
        """
        Check whether or not a given trigger exists

        :param schema_name: The schema of the trigger
        :param trigger_name: the trigger name
        :param db: the SQLAlchemy session
        :return: whether exists or not
        """
        from shared.models.mlflow_models import SysSchemas, SysTriggers
        x = db.query(SysTriggers). \
            join(SysSchemas, SysTriggers.SCHEMAID == SysSchemas.SCHEMAID). \
            filter((func.upper(SysSchemas.SCHEMANAME) == schema_name.upper()) &
                   (func.upper(SysTriggers.TRIGGERNAME) == trigger_name.upper())). \
            count()
        return bool(x)

    @staticmethod
    def drop_table_if_exists(schema_name: str, table_name: str, db: Session):
        """
        Drops table if exists
        :param schema_name: schema name
        :param table_name: the table name
        :param engine: the SQLAlchemy Session
        """
        # We are using the execute so we can preserve the session object, instead of having to use the engine,
        #  which is NOT thread safe
        db.execute(f'Drop table if exists {schema_name}.{table_name}')

    @staticmethod
    def drop_trigger_if_exists(schema_name: str, trigger_name: str, db: Session):
        """
        Drops a trigger if it exists

        :param schema_name: The schema of the trigger
        :param trigger_name: Name of the trigger
        :param db: SQLAlchemy Session (NOT engine)
        """
        if DatabaseFunctions.trigger_exists(schema_name, trigger_name, db):
            db.execute(f'DROP TRIGGER {schema_name}.{trigger_name}')

    @staticmethod
    def drop_trigger_if_exists(schema_name: str, trigger_name: str, db):
        """
        Drops trigger if exists
        :param trigger_name: trigger name
        :param db: the SQLAlchemy session
        """
        if DatabaseFunctions.trigger_exists(schema_name, trigger_name, db):
            db.execute(SQL.drop_trigger.format(trigger_name=f'{schema_name}.{trigger_name}'))
