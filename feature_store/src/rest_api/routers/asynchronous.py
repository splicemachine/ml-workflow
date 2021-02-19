# from fastapi import APIRouter, status

# from shared.logger import logger
# from shared.models.user_entities import (CreatedAsyncTask, HealthReport,
#                                          InteractionReport, User)

# from .auth import AUTH_USER

# # Asynchronous API Router -- mountable to main API
# ASYNC_ROUTER = APIRouter()

# GENERAL_ASYNC_PARAMS = dict(
#     response_model=CreatedAsyncTask,
#     status_code=status.HTTP_202_ACCEPTED
# )


# @ASYNC_ROUTER.post('/report-interaction', operation_id="queue_interaction_report",
#                    description="Log contact  between a user and multiple users", **GENERAL_ASYNC_PARAMS)
# async def queue_interaction_report(contact: InteractionReport, user: User = AUTH_USER):
#     """
#     Queue an interaction report asynchronously in the backend via service
#     * Specify the interaction model in the JSON body (schema in docs)
#     * Requires JWT
#     """
#     logger.info(f"Processing Interaction Report Task...")
#     return CreatedAsyncTask(task_id=user.queue_task(task_name='tasks.report_interaction', task_data=contact))


# @ASYNC_ROUTER.post('/report-health', operation_id='queue_health_report',
#                    description="Log health report under the specified user", **GENERAL_ASYNC_PARAMS)
# async def queue_health_report(health_report: HealthReport, user: User = AUTH_USER):
#     """
#     Queue a health report asynchronously in the backend via service
#     * Specify the health report model in the JSON Body
#     * Requires JWT
#     """
#     logger.info(f"Processing Health Reporting Task...")
#     return CreatedAsyncTask(task_id=user.queue_task(task_name='tasks.report_health', task_data=health_report))


# @ASYNC_ROUTER.post('/set-active-user', operation_id='queue_set_active_user',
#                    description="Notify the system that a user has logged in for the first time", **GENERAL_ASYNC_PARAMS)
# async def set_active_user(user: User = AUTH_USER):
#     """
#     Queue a change in the specified user's state in the database
#     * Specify info model in the request body
#     * Requires JWT
#     """
#     logger.info(f"Processing User Status Notification Task....")
#     return CreatedAsyncTask(task_id=user.queue_task(task_name='tasks.report_active_user'))