"""
Utilities for the MLManager Director UI
"""
from shared.api.models import TrackerTableMapping
from sqlalchemy import text

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2019, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja", "Ben Epstein"]

__license__: str = "Commercial"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"
__status__: str = "Quality Assurance (QA)"


def handle_bootgrid_query(session, request, executor) -> dict:
    """
    As a temporary workaround,
    we use SQL instead of SQLAlchemy
    since driver does not support offset yet.
    Since this is also a fairly complex query--
    SQL is more efficient
    :param session: sqlalchemy session
    :param request: the request
    :param executor: parallel async executor for Flask
    :return: (dict) JSON response rendered in front end
    """
    job_table: str = "JOBS"

    # Parse Table Order Information
    order_arg: str = list(filter(lambda key: key.startswith('sort'), request.form))[0]
    order_value: str = order_arg.split('[')[1].split(']')[0]  # parse sort[column] -> column

    direction_suffix: str = "DESC" if request.form[order_arg] == 'desc' else ''
    limit: int = int(request.form['rowCount']) if request.form.get('rowCount') != '-1' else 0

    # AJAX from jquery-bootgrid has -1 if user selects no limit
    if request.form['searchPhrase']:  # query is doing a search on HTML table
        int_offset: int = 0  # no offset on searches
        table_query: text = _get_job_search_query(job_table, order_value, direction_suffix, limit,
                                                  request.form['searchPhrase'])
    else:  # query is listing
        int_offset: int = int(request.form['current']) - 1 if request.form.get('current') else 0
        table_query: text = _get_job_list_query(job_table, order_value, direction_suffix, limit,
                                                int_offset)

    total_query: text = f"""SELECT COUNT(*) FROM {job_table}"""  # how many pages to make in js

    futures: list = [
        executor.submit(lambda: [_preformat_job_row(row) for row in session.execute(table_query)]),
        executor.submit(lambda: list(session.execute(total_query))[0][0])
    ]

    table_data, total_rows = futures[0].result(), futures[1].result()  # block until we get results

    # Add Job Logs Links
    for row in table_data:
        row[TrackerTableMapping.job_logs] = f"<a href='/watch/{row[TrackerTableMapping.id_col]}'>View Logs</a>"

    return dict(rows=table_data,
                current=int_offset + 1,
                total=total_rows,
                rowCount=limit)


def _preformat_job_row(job_row: list) -> dict:
    """
    Format job row object to have some
    columns preformatted (<pre></pre> for table rendering
    :param job_row: (ResultProxy) the Job Row to format
    :return: (dict) formatted row
    """
    job_object: dict = dict(job_row)
    for column in TrackerTableMapping.preformatted_columns:
        if column in job_object:
            job_object[column] = f'<pre>{job_object[column]}</pre>'
    return job_object


def _get_job_search_query(job_table: str, order_col: str, direction: str, limit: int,
                          search_term: str) -> text:
    """
    Get SQL Query to search columns for search string

    :param job_table: (str) Table in DB where jobs are stored
    :param order_col: (str) column to sort by
    :param direction: (str) either desc or nothing (ascending)
    :param limit: (int) number of rows to retrieve
    :param search_term: (str) term to look for in searchable columns
    :return: (text) SELECT statement
    """
    limit_clause: str = f'FETCH FIRST {limit} ROWS ONLY' if limit > 0 else ''

    filter_clause: str = f" LIKE '%{search_term}%' OR "
    filter_expression: str = filter_clause.join(TrackerTableMapping.searchable_columns) + filter_clause[:-4]  # cut off
    # OR on last column

    return text(
        f"""
        SELECT {TrackerTableMapping.sql_columns} FROM {job_table}
        WHERE {filter_expression} 
        ORDER BY {TrackerTableMapping.DB_MAPPING[order_col]} {direction}
        {limit_clause}
        """
    )


def _get_job_list_query(job_table: str, order_col: str, direction: str, limit: int,
                        offset: int) -> text:
    """
    Get JQuery Bootgrid formatted JSON (no search) for
    rendering in HTML Table for /tracker.

    :param job_table: (str) Table in DB where jobs are stored
    :param order_col: (str) column to sort by
    :param direction: (str) either desc or nothing (ascending)
    :param limit: (int) number of rows to retrieve
    :param offset: (int) number of rows to skip
    :return: (text) SELECT statement
    """
    limit_clause: str = f'FETCH NEXT {limit} ROWS ONLY' if limit > 0 else ''
    return text(
        f"""
        SELECT {TrackerTableMapping.sql_columns} FROM {job_table}
        ORDER BY {TrackerTableMapping.DB_MAPPING[order_col]} {direction}
        OFFSET {offset} ROWS
        {limit_clause}
        """
    )
