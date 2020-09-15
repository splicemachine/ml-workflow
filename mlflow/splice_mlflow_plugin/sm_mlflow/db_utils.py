"""
Database + SQLAlchemy Utilities for Splice MLFlow Support
"""
import re

import sqlalchemy.sql.expression as sql
from sqlalchemy.sql.sqltypes import Integer

from mlflow.exceptions import MlflowException
from mlflow.protos.databricks_pb2 import INVALID_PARAMETER_VALUE
from mlflow.store.tracking.dbmodels.models import (SqlLatestMetric, SqlParam,
                                                   SqlRun, SqlTag)
from mlflow.utils.search_utils import SearchUtils


def fix_CASE_clause(self, sql: str) -> str:
    """
    Splice Machine has 2 limitations here:
    1. We don't support ? in all CASE statement returns: ERROR 42X87:
        At least one result expression (THEN or ELSE) of the 'conditional' expression must not be a '?
    2. We don't support IS as a boolean operator (CASE WHEN x IS 5)
    Fix for 1. Add a cast to the ELSE
    Fix for 2. Check for IS or IS NOT
    :param sql: (str) the SQL
    :return: (str) the fixed SQL
    """
    reg = 'CASE(.*)END'
    result = re.search(reg, sql)
    CASE_SQL = result.group(1)

    indexes = [m.start() for m in re.finditer('THEN', CASE_SQL)] + [re.search('ELSE', CASE_SQL).start()]
    needs_cast = True
    for i in indexes:
        # Check for all "THEN ?" or "ELSE ?"
        if CASE_SQL[i + 5] != '?':
            needs_cast = False
            break
    if needs_cast:
        new_CASE_SQL = CASE_SQL.replace('ELSE ?', 'ELSE CAST(? as INT)')

    # Replace IS <> with = <>
    # We rerun the search every time because after each replacement, the length of the string has changed
    is_clause = 'IS [^NULL]'
    x = re.search(is_clause, new_CASE_SQL)
    while x:
        ind = x.start()
        new_CASE_SQL = new_CASE_SQL[:ind] + '=' + new_CASE_SQL[ind + 2:]  # skip IS
        x = re.search(is_clause, new_CASE_SQL)

    # Replace IS NOT <> with = <>
    # We rerun the search every time because after each replacement, the length of the string has changed
    is_not_clause = 'IS NOT [^NULL]'
    x = re.search(is_not_clause, new_CASE_SQL)
    while x:
        ind = x.start()
        new_CASE_SQL = new_CASE_SQL[:ind] + '!=' + new_CASE_SQL[ind + 6:]  # skip IS NOT
        x = re.search(is_clause, new_CASE_SQL)

    fixed_sql = sql.replace(CASE_SQL, new_CASE_SQL)
    return fixed_sql


def get_orderby_clauses_test(order_by, session):
    """Sorts a set of runs based on their natural ordering and an overriding set of order_bys.
    Runs are naturally ordered first by start time descending, then by run id for tie-breaking.
    """

    clauses = []
    ordering_joins = []
    clause_id = 0
    # contrary to filters, it is not easily feasible to separately handle sorting
    # on attributes and on joined tables as we must keep all clauses in the same order
    if order_by:
        for order_by_clause in order_by:
            clause_id += 1
            (key_type, key, ascending) = SearchUtils.parse_order_by(order_by_clause)
            if SearchUtils.is_attribute(key_type, '='):
                order_value = getattr(SqlRun, SqlRun.get_attribute_name(key))
            else:
                if SearchUtils.is_metric(key_type, '='):  # any valid comparator
                    entity = SqlLatestMetric
                elif SearchUtils.is_tag(key_type, '='):
                    entity = SqlTag
                elif SearchUtils.is_param(key_type, '='):
                    entity = SqlParam
                else:
                    raise MlflowException("Invalid identifier type '%s'" % key_type,
                                          error_code=INVALID_PARAMETER_VALUE)

                # build a subquery first because we will join it in the main request so that the
                # metric we want to sort on is available when we apply the sorting clause
                subquery = session \
                    .query(entity) \
                    .filter(entity.key == key) \
                    .subquery()

                ordering_joins.append(subquery)
                order_value = subquery.c.value

            # sqlite does not support NULLS LAST expression, so we sort first by
            # presence of the field (and is_nan for metrics), then by actual value
            # As the subqueries are created independently and used later in the
            # same main query, the CASE WHEN columns need to have unique names to
            # avoid ambiguity
            if SearchUtils.is_metric(key_type, '='):
                clauses.append(sql.case([
                    (subquery.c.is_nan == True, 1),
                    (order_value.is_(None), 1)
                ], else_=0).cast(Integer).label('clause_%s' % clause_id))
            else:  # other models do not have an 'is_nan' field
                clauses.append(sql.case([(order_value.is_(None), 1)], else_=0).cast(Integer)
                               .label('clause_%s' % clause_id))

            if ascending:
                clauses.append(order_value)
            else:
                clauses.append(order_value.desc())

    clauses.append(SqlRun.start_time.desc())
    clauses.append(SqlRun.run_uuid)
    return clauses, ordering_joins
