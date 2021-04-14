from os import environ as env_vars

from fastapi import FastAPI, status, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi_utils.timing import add_timing_middleware
from uvicorn import run as run_server
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError

# from routers.asynchronous import ASYNC_ROUTER
from .routers.synchronous import SYNC_ROUTER
from shared.logger.logging_config import logger
from shared.api.exceptions import SpliceMachineException, ExceptionCodes
from starlette.exceptions import HTTPException as StarletteHTTPException

APP: FastAPI = FastAPI(
    title="Feature Store API",
    root_path=env_vars.get('ROOT_PATH','/'), # Because we run behind a proxy in the cloud, it will be /featurestore
    debug=env_vars.get('DEBUG', False),
    description="API for asynchronous and synchronous calls to the feature store"
)
add_timing_middleware(app=APP, record=logger.info, exclude='health')
# Add CORS support from production and test domains
APP.add_middleware(
    middleware_class=CORSMiddleware,
    allow_origins=[
        'http://localhost:8090',
        'http://localhost:3000',
        'https://localhost:3000',
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

@APP.on_event(event_type='startup')
def on_startup():
    """
    Runs when the API Server starts up
    """
    logger.info("Getting database connection")
    from shared.services.database import SQLAlchemyClient
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
        content={ "code": ExceptionCodes.BAD_ARGUMENTS, "message": str(exc) }
    )

@APP.exception_handler(SpliceMachineException)
def splice_machine_exception_handler(request: Request, exc: SpliceMachineException):
    logger.error(exc)
    return JSONResponse(
        status_code=exc.status_code,
        content={ "code": exc.code, "message": exc.message },
    )

@APP.exception_handler(StarletteHTTPException)
def http_exception_handler(request: Request, exc: StarletteHTTPException):
    logger.error(exc)
    return JSONResponse(
        status_code=exc.status_code,
        content={ "code": ExceptionCodes.UNKNOWN, "message": exc.detail },
    )


@APP.get('/health', description='Health check', response_model=str, operation_id='healthcheck', tags=['Mgmt'],
         status_code=status.HTTP_200_OK)
def health_check():
    """
    Returns 'OK'
    """
    logger.info('Getting DB')
    from shared.services.database import SQLAlchemyClient
    sess = SQLAlchemyClient.SessionMaker()
    logger.info('Testing connection')
    x = sess.execute('select top 1 * from sys.systables').fetchall()
    logger.info('Closing DB session')
    sess.commit()
    sess.close()
    return 'OK'

# APP.include_router(
#     router=ASYNC_ROUTER,
#     tags=['Async']
# )

APP.include_router(
    router=SYNC_ROUTER
    # tags=['Sync']
)


