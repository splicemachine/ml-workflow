import os
import traceback

from fastapi import Depends, FastAPI, Request, status
from fastapi.responses import JSONResponse
from fastapi_utils.timing import add_timing_middleware, record_timing
from routers.artifact_router import ARTIFACT_ROUTER
from routers.job_router import JOB_ROUTER
from routers.monitoring_router import MONITORING_ROUTER
from starlette.exceptions import HTTPException as StarletteHTTPException

from shared.api.auth_dependency import authenticate
from shared.api.exceptions import ExceptionCodes, SpliceMachineException
from shared.db.connection import SQLAlchemyClient
from shared.logger.logging_config import logger

custom_cors = os.environ.get('ENABLE_CORS_URL')
custom_cors = custom_cors.split(',') if custom_cors else []

APP: FastAPI = FastAPI(
    title="MLManager Director API",
    root_path=os.environ.get('ROOT_PATH', '/'),
    debug=os.environ.get('DEBUG', False),
    description="API for asynchronous and synchronous calls to the MLManager Director API",
    dependencies=[Depends(authenticate)],
    allow_origins=custom_cors
)

add_timing_middleware(app=APP, record=logger.info, exclude='health')


@APP.on_event(event_type='startup')
def on_startup():
    """
    Runs when the API Server starts up
    """
    logger.info("Getting database connection")
    SQLAlchemyClient.connect()  # cache for future operations
    logger.info("****** API IS STARTING UP ******")


@APP.on_event(event_type='shutdown')
def on_shutdown():
    """
    Runs when the API Server shuts down
    """
    logger.info("****** API IS SHUTTING DOWN ******")


@APP.get('/health', summary="Health Check", response_model=str, operation_id='health', status_code=status.HTTP_200_OK)
def health_check():
    """
    Make sure that the server is alive
    """
    return 'Ok'


@APP.exception_handler(SpliceMachineException)
def splice_machine_exception_handler(request: Request, exc: SpliceMachineException):
    logger.error(traceback.print_tb(exc.__traceback__))
    return JSONResponse(
        status_code=exc.status_code,
        content={"code": exc.code, "message": exc.message}
    )


@APP.exception_handler(StarletteHTTPException)
def http_exception_handler(request: Request, exc: StarletteHTTPException):
    logger.error(traceback.format_tb(exc.__traceback__))
    return JSONResponse(
        status_code=exc.status_code,
        content={"code": ExceptionCodes.UNKNOWN, "message": exc.detail}
    )


APP.include_router(
    router=MONITORING_ROUTER,
    prefix='/monitoring',
    tags=['Monitoring']
)

APP.include_router(
    router=JOB_ROUTER,
    prefix='/jobs',
    tags=['Job Management']
)

APP.include_router(
    router=ARTIFACT_ROUTER,
    prefix='/artifacts',
    tags=['Artifacts']
)
