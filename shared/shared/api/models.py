"""
Definitions for Flask API
"""
__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2019, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja"]

__license__: str = "Proprietary"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"


class APIStatuses:
    """
    Class containing valid
    HTTP Response statuses from Flask
    """
    success: str = "success"
    failure: str = "failure"
    pending: str = "submitted"
    error: str = "error"


class TrackerTableMapping:
    """
    Valid Tracker Table Mappings
    to Database columns
    """
    id_col: str = "ID"
    timestamp: str = "TIMESTAMP"
    user: str = "USER"
    handler_name: str = "HANDLER_NAME"
    payload: str = "PAYLOAD"
    status: str = "STATUS"
    info: str = "INFO"
    mlflow_url: str = "MLFLOW_URL"
    target_service: str = "TARGET_SERVICE"

    # right now, the ids should correspond with ids in jquery bootgrid
    DB_MAPPING: dict = {
        id_col: 'ID',
        timestamp: '"timestamp"',  # must be quoted as it is reserved in SQL
        user: '"user"',
        handler_name: '"HANDLER_NAME"',  # capitalized since it is a backref
        payload: "payload",
        status: '"STATUS"',
        info: "info",
        mlflow_url: "mlflow_url",
        target_service: 'TARGET_SERVICE'
    }

    sql_columns: str = ','.join(  # cannot use .keys() as hash tables are not stored in order
        (
            DB_MAPPING[id_col],
            DB_MAPPING[timestamp],
            DB_MAPPING[user],
            DB_MAPPING[handler_name],
            DB_MAPPING[payload],
            DB_MAPPING[status],
            DB_MAPPING[info],
            DB_MAPPING[mlflow_url],
            DB_MAPPING[target_service]
        )
    )

    searchable_columns: list = [  # LIKE requires quoted column names if not
        # reserved (then it is double quoted)
        f"'{identifier}'" if not identifier.startswith('"') else identifier
        for identifier in
        [
            DB_MAPPING[id_col],
            DB_MAPPING[timestamp],
            DB_MAPPING[user],
            DB_MAPPING[handler_name],
            DB_MAPPING[status]
        ]
    ]

    preformatted_columns: list = [  # columns whose values need to be surrounded with <pre></pre>
        payload,
        info
    ]
