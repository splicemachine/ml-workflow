from utils.pipeline_utils.pipeline_utils import create_default_pipes
from shared.logger.logging_config import logger
from shared.models.feature_store_models import (create_feature_store_tables,
                                                wait_for_runs_table)


def setup():
    """
    A function that runs before starting up the API server that creates all of the FeatureStore tables in a single thread
    to avoid write-write conflicts.
    """
    wait_for_runs_table()
    logger.info("Creating Feature Store Tables...")
    create_feature_store_tables()
    logger.info("Tables are created")

if __name__ == '__main__':
    setup()

    logger.info("Creating default Pipes")
    from typing import List, Dict
    from inspect import getsource
    import cloudpickle

    import schemas
    from utils.utils import stringify_bytes

    pipes = []

    def create_aggregates(df, windows: List[str], functions: List[str], columns: Dict[str, str], primary_keys: List[str], ts_col: str):
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window
        import re

        time_conversions = {
            "m":60,
            "h":60*60,
            "d":60*60*24,
            "w":60*60*24*7,
            "mn":60*60*24*30, # 30 days in a month
            "q":60*60*24*7*13,  # 13 week in a quarter
            "y":60*60*24*365 # 365 days in a year
        }

        valid_windows = ["s", "m", "h", "d", "w", "mn", "q", "y"]

        for window in windows:
            wlength, wtype = re.match(r"(\d+)(\w+)", window).groups()
            if wtype not in valid_windows:
                raise ValueError(f'{window} is not a valid window. Please use AggWindow.is_valid() to validate your window before adding it as a parameter this pipe.')
            offset = -time_conversions.get(wtype) * int(wlength)

            for function in functions:
                if function == 'sum':
                    func = F.sum
                elif function == 'count':
                    func = F.count
                elif function == 'avg':
                    func = F.avg
                elif function == 'min':
                    func = F.min
                elif function == 'max':
                    func = F.max
                else:
                    raise ValueError(f'Function {function} is not recognized. Please use one of the functions present in FeatureAgg.get_valid()')

                w = (Window()
                    .partitionBy(*[F.col(pk) for pk in primary_keys])
                    .orderBy(F.col(ts_col).cast('long'))
                    .rangeBetween(offset, 0))

                for feature, prefix in columns.items():
                    df = df.withColumn(f'{prefix}_{function}_{window}', func(feature).over(w))
                
        return df

    pipe = schemas.PipeCreate(
        name='window_aggregation', 
        description='A pipe for generating Features as Aggregations of data over a specified windows of time.',
        ptype='B',
        lang='pyspark',
        func=stringify_bytes(cloudpickle.dumps(create_aggregates)),
        code=getsource(create_aggregates)
    )

    pipes.append(pipe)
    create_default_pipes(pipes)

    logger.info("Default Pipes created")
