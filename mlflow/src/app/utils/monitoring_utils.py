from sqlalchemy.orm import load_only
from shared.models.splice_models import Job


def get_logs(task_id: int, session):
    """
    Retrieve the logs for a given task id
    :param task_id: the celery task id to get the logs for
    """
    job = session.query(Job).options(load_only("logs")).filter_by(id=task_id).one()
    return job.logs.split('\n')
