from alembic.ddl import base
from alembic.ddl.base import ColumnType, RenameTable, ColumnName, ColumnNullable, alter_table, \
    format_column_name

from alembic.ddl.impl import DefaultImpl

from sqlalchemy.ext.compiler import compiles

DUMMY_QUERY: str = 'VALUES 1'
AVAILABLE_TRANSFER_TYPES: tuple = (
    'VARCHAR', 'LONGVARCHAR', 'BLOB', 'CLOB'
)  # columns which can have their length mutated


class SpliceMachineImpl(DefaultImpl):
    """
    Splice Machine Alembic Implementation
    """
    __dialect__ = 'splicemachinesa'
    transactional_ddl = True

    def execute(self, sql, execution_options=None):
        try:
            print("Executing: " + str(sql))
            super().execute(sql, execution_options)
        except:
            import traceback
            print(traceback.format_exc())


@compiles(ColumnType, 'splicemachinesa')
def visit_column_type(element, compiler, **kw) -> str:
    """
    TODO @amrit: This is an extremely hacky temporary fix for our databases lack of support
    of Altering the length of non string-based columns. We essentially just ignore length changes,
    which is fine for MLFlow, but not if we are sending this code out to production
    """

    if str(element.type_) in AVAILABLE_TRANSFER_TYPES:
        print("Transfer type " + (str(element.type_)))
        data_type = base.format_type(compiler, element.type_)
        return "%s %s %s" % (
            base.alter_table(compiler, element.table_name, element.schema),
            base.alter_column(compiler, element.column_name),
            "SET DATA TYPE %s" % data_type
        )
    print("Ignoring Query")
    return ";"  # just a placeholder to indicate no action should be taken


@compiles(ColumnName, 'splicemachinesa')
def visit_column_name(element, compiler, **kw) -> str:
    return "%s RENAME COLUMN %s TO %s" % (
        base.alter_table(compiler, element.table_name, element.schema),
        base.format_column_name(compiler, element.column_name),
        base.format_column_name(compiler, element.newname)
    )


@compiles(RenameTable, 'splicemachinesa')
def visit_rename_table(element, compiler, **kw) -> str:
    return "RENAME TABLE %s TO %s" % (
        base.format_table_name(compiler, element.table_name, element.schema),
        base.format_table_name(compiler, element.new_table_name, element.schema)
    )


@compiles(ColumnNullable, "splicemachinesa")
def visit_column_nullable(element, compiler, **kw) -> str:
    return "%s %s %s" % (
        alter_table(compiler, element.table_name, element.schema),
        alter_column(compiler, element.column_name),
        "NULL" if element.nullable else "NOT NULL",
    )


def alter_column(compiler, name) -> str:
    return "ALTER COLUMN %s" % format_column_name(compiler, name)


def add_column(compiler, column, **kw) -> str:
    return "ADD COLUMN%s" % compiler.get_column_specification(column, **kw)
