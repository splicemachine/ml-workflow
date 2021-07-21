from os import environ as env_vars

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from fastapi_utils.timing import add_timing_middleware, record_timing
from starlette.exceptions import HTTPException as StarletteHTTPException

from shared.api.exceptions import ExceptionCodes, SpliceMachineException
from shared.db.connection import SQLAlchemyClient
from shared.logger.logging_config import logger

# from routers.asynchronous import ASYNC_ROUTER
from routers.synchronous import SYNC_ROUTER
from utils.airflow_utils import Airflow

APP: FastAPI = FastAPI(
    title="Feature Store API",
    root_path=env_vars.get('ROOT_PATH', '/'),  # Because we run behind a proxy in the cloud, it will be /featurestore
    debug=env_vars.get('DEBUG', False),
    description="API for asynchronous and synchronous calls to the feature store"
)
add_timing_middleware(app=APP, record=logger.info, exclude='health')
# Add CORS support from production and test domains
origins = [  # FIXME: Remove in the future
    'http://localhost:8090',
    'http://localhost:3000',
    'https://localhost:3000',
]
custom_cors = env_vars.get('ENABLE_CORS_URL')
custom_cors = custom_cors.split(',') if custom_cors else []
origins += custom_cors
APP.add_middleware(
    middleware_class=CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

APP.add_middleware(GZipMiddleware, minimum_size=1e6)  # 1MB


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


@APP.exception_handler(RequestValidationError)
def validation_exception_handler(request: Request, exc: RequestValidationError):
    logger.error(exc)
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={"code": ExceptionCodes.BAD_ARGUMENTS, "message": str(exc)}
    )


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


@APP.get('/health', description='Health check', response_model=str, operation_id='healthcheck', tags=['Mgmt'],
         status_code=status.HTTP_200_OK)
def health_check():
    """
    Returns 'OK'
    """
    logger.info('Getting DB')
    from shared.db.connection import SQLAlchemyClient
    sess = SQLAlchemyClient().SessionMaker()
    logger.info('Testing connection')
    x = sess.execute('select top 1 * from sys.systables').fetchall()
    logger.info('Closing DB session')
    sess.commit()
    sess.close()
    return 'OK'



APP.include_router(
    router=SYNC_ROUTER
    # tags=['Sync']
)

Airflow.setup()
