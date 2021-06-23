import os

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from shared.logger.logging_config import logger
from fastapi_utils.timing import add_timing_middleware, record_timing

from shared.shared.api.exceptions import ExceptionCodes
from shared.db.connection import SQLAlchemyClient
from shared.shared.api.exceptions import SpliceMachineException
from starlette.exceptions import HTTPException as StarletteHTTPException

from .routers.job_router import JOB_ROUTER
from .routers.artifact_router import ARTIFACT_ROUTER
from .routers.monitoring_router import MONITORING_ROUTER

APP: FastAPI = FastAPI(
    title="MLManager Director API",
    debug=os.environ.get('DEBUG', False),
    description="API for asynchronous and synchronous calls to the MLManager Director API",
    dependencies=[]
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


@APP.get('/health', summary="Health Check", response_model=str, status_code=status.HTTP_200_OK)
async def health_check():
    return 'Ok'


@APP.exception_handler(SpliceMachineException)
def splice_machine_exception_handler(request: Request, exc: SpliceMachineException):
    logger.error(exc)
    return JSONResponse(
        status_code=exc.status_code,
        content={"code": exc.code, "message": exc.message}
    )


@APP.exception_handler(StarletteHTTPException)
def http_exception_handler(request: Request, exc: StarletteHTTPException):
    logger.error(exc)
    return JSONResponse(
        status_code=exc.status_code,
        content={"code": ExceptionCodes.UNKNOWN, "message": exc.detail}
    )


APP.include_router(
    router=MONITORING_ROUTER,
    prefix='monitoring',
    tags=['Monitoring']
)

APP.include_router(
    router=JOB_ROUTER,
    prefix='jobs',
    tags=['Job Management']
)

APP.include_router(
    router=ARTIFACT_ROUTER,
    prefix='artifacts',
    tags=['Artifacts']
)
