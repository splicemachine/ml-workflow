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
from shared.models.feature_store_models import wait_for_runs_table, create_feature_store_tables

APP: FastAPI = FastAPI(
    title="Feature Store API",
    debug=env_vars.get('DEBUG', False),
    description="API for asynchronous and synchronous calls to the feature store"
)
add_timing_middleware(app=APP, record=logger.info, exclude='health')
# Add CORS support from production and test domains
APP.add_middleware(
    middleware_class=CORSMiddleware,
    allow_origins=[
        'http://localhost:8090'
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

@APP.on_event(event_type='startup')
async def on_startup():
    """
    Runs when the API Server starts up
    """
    logger.info("****** API IS STARTING UP ******")


@APP.on_event(event_type='shutdown')
async def on_shutdown():
    """
    Runs when the API Server shuts down
    """
    logger.info("****** API IS SHUTTING DOWN ******")

@APP.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    logger.error(exc)
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST, 
        content={ "code": ExceptionCodes.BAD_ARGUMENTS, "message": str(exc) }
    )

@APP.exception_handler(SpliceMachineException)
async def splice_machine_exception_handler(request: Request, exc: SpliceMachineException):
    logger.error(exc)
    return JSONResponse(
        status_code=exc.status_code,
        content={ "code": exc.code, "message": exc.message },
    )

@APP.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    logger.error(exc)
    return JSONResponse(
        status_code=exc.status_code,
        content={ "code": ExceptionCodes.UNKNOWN, "message": exc.detail },
    )


@APP.get('/health', description='Health check', response_model=str, operation_id='healthcheck', tags=['Mgmt'],
         status_code=status.HTTP_200_OK)
async def health_check():
    """
    Returns 'OK'
    """
    return 'OK'

# APP.include_router(
#     router=ASYNC_ROUTER,
#     tags=['Async']
# )

APP.include_router(
    router=SYNC_ROUTER
    # tags=['Sync']
)

def setup():
    wait_for_runs_table()
    logger.info("Creating Feature Store Tables...")
    create_feature_store_tables()

setup()

if __name__ == '__main__':
    run_server(APP, host='0.0.0.0', port=8798)
