import re
from typing import List, Tuple

from fastapi import status

from shared.api.exceptions import ExceptionCodes, SpliceMachineException

import utils.pipeline_utils.constants as c


def build_agg_feature_name(feature_prefix: str, agg_func: str, agg_window: str) -> str:
    """
    Creates the feature name for the aggregation feature based on the prefix, aggregation function and window

    :param feature_prefix: The prefix
    :param agg_func: The aggregation function (SUM, MIN, COUNT etc)
    :param agg_window: The window (1d, 5m, 2w etc)
    :return: str the full aggregation feature name
    """
    return f'{feature_prefix}_{agg_func}_{agg_window}'

def build_agg_expression( agg_function: str, agg_window: str, source_column: str,
                          source_ts_column: str, as_of_ts_expr: str, default_value: float = None )-> str:
    """
    Builds out the aggregation expressions (AVG, SUM, COUNT etc) for the provided function, window and column. Also
    provides the expression in a time consistent way, and provides a default value in case of null for that time window

    :param agg_function: The SQL aggregation
    :param agg_window: The aggregate window (5d, 2w, 5mo etc)
    :param source_column:
    :param source_ts_column:
    :param as_of_ts_expr:
    :param default_value:
    :return:
    """
    window_type, window_length = parse_time_window(agg_window)
    tsi_constant = c.tsi_windows.get(window_type) # Convert the window type to a Splice Machine Timestamp type

    case = f'CASE WHEN ' \
           f'TIMESTAMPDIFF({tsi_constant}, x.{source_ts_column}, TIMESTAMP({as_of_ts_expr}) ) BETWEEN 1 AND {window_length} ' \
           f'THEN {source_column} END'

    if (agg_function.lower()=='avg'):
        result = f'COALESCE(SUM({case}) / {window_length}'
    else:
        result = f'COALESCE({agg_function}( {case} )'
    default_text = f'{default_value if default_value != None else "NULL"}'
    result = f'{result}, {default_text} )'
    result = f'CAST( ({result}) AS DOUBLE )'
    return result


def parse_time_window(time_window: str)-> Tuple[str, int]:
    """
    Splits
       52d into d, 52
       1w  into w, 1

    :param time_window: (str) the window
    :return: Tuple(str, int) the time window split
    """
    window_length, window_type = re.match(r"(\d+)(\w+)", time_window).groups()
    if window_type not in c.tsi_windows.keys():
        raise SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.BAD_ARGUMENTS,
                                     message=f'The provided window {window_type} is invalid. '
                                             f'The window unit must be one of {c.tsi_windows.keys()}')
    return (window_type, int(window_length))

def convert_window_to_seconds(agg_window) -> int:
    """
    Gets the time window converted to seconds
    ie 2d (2 days) would be 172800
    :param agg_window: The window aggregation (2d, 1w, 3mn etc)
    :return:
    """
    window, length = parse_time_window(agg_window)
    return (c.time_conversions.get(f's-{window}') * length)

def _convert_window_to_sql_interval(agg_window) -> str:
    """
    Converts the window provided to a SQL compliant interval
    :param agg_window: The window aggregation (2d, 1w, 3mn etc)
    :return: The SQL compliant interval of that window
    """
    window, length = parse_time_window(agg_window)
    # Week and quarter aren't support in Splice Machine so for week we use 7*length day, and quarter is 3*length month
    if window == 'q':
        length *= 3
    if window == 'w':
        length *= 7
    sql_unit = c.window_to_interval_keyword.get(window)
    return f'{length} {sql_unit}'

def _get_max_interval(windows = List[str]):
    """
    Gets the max interval provided by the user by converting all time windows to seconds and picking the largest
    :param windows: List of windows for aggregations
    :return: The largest one
    """
    # convert all windows to seconds for comparison
    window_sizes = [convert_window_to_seconds(w) for w in windows]
    # Get the index of the max window size, which is the same index as the max window
    return windows[window_sizes.index(max(window_sizes))]

def get_largest_window_sql(windows = List[str]):
    """
    Finds the largest window to create lower bound of time for source table scan and returns that window in a SQL
    compliant syntax

    :param windows: the time windows provided by the user (2d, 3mn, 10q etc)
    :return: The SQL complaint syntax of the largest window
    """
    max_window = _get_max_interval(windows)
    return _convert_window_to_sql_interval(max_window)

def _get_min_interval(windows = List[str]):
    """
    Gets the min interval provided by the user by converting all time windows to seconds and picking the smallest
    :param windows: List of windows for aggregations
    :return: The smallest one
    """
    # convert all windows to seconds for comparison
    window_sizes = [convert_window_to_seconds(w) for w in windows]
    # Get the index of the max window size, which is the same index as the max window
    return windows[window_sizes.index(min(window_sizes))]

def get_min_window_sql(windows=List[str]) -> Tuple[int, int]:
    """
    Finds the smallest window provided by the user and returns it in a SQL compliant syntax (the window type as an int
    representation and the interval value)

    example: get_min_window_sql(['2d','3s','1w']) -> (3,1)
    This is because the smallest window is '3s' (3 seconds) and second is represented by a 1 (constants.tsi_window_values)

    :param windows: List of windows for aggregations
    :return: the smallest one in the form Tuple(min_window_type, min_window_length)
    """
    min_window = _get_min_interval(windows)
    min_window_type, min_window_length = parse_time_window(min_window)
    return (min_window_length, c.tsi_window_values.get(min_window_type))
