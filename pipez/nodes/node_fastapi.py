import importlib.resources
import logging
from abc import ABC, abstractmethod
from typing import Optional

import uvicorn
from fastapi import APIRouter, FastAPI, Request
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.openapi.docs import get_redoc_html, get_swagger_ui_html, get_swagger_ui_oauth2_redirect_html
from fastapi.staticfiles import StaticFiles

from ..core.batch import Batch
from ..core.node import Node


class NodeFastAPI(Node, ABC):
    def __init__(
            self,
            host: str = '0.0.0.0',
            port: int = 80,
            **kwargs
    ):
        super().__init__(**kwargs)
        self._host = host
        self._port = port
        self._app = None
        self._router = None

    def post_init(self):
        self._app = FastAPI(docs_url=None, redoc_url=None)
        self._router = APIRouter()

        directory = importlib.resources.files('pipez.resources') / 'fastapi'
        self._app.mount('/static', StaticFiles(directory=directory), name='static')

        @self._app.get('/docs', include_in_schema=False)
        async def custom_swagger_ui_html():
            return get_swagger_ui_html(openapi_url=self._app.openapi_url,
                                       title=self._app.title,
                                       oauth2_redirect_url=self._app.swagger_ui_oauth2_redirect_url,
                                       swagger_js_url='/static/swagger-ui-bundle.js',
                                       swagger_css_url='/static/swagger-ui.css')

        @self._app.get(self._app.swagger_ui_oauth2_redirect_url, include_in_schema=False)
        async def swagger_ui_redirect():
            return get_swagger_ui_oauth2_redirect_html()

        @self._app.get('/redoc', include_in_schema=False)
        async def redoc_html():
            return get_redoc_html(openapi_url=self._app.openapi_url,
                                  title=self._app.title,
                                  redoc_js_url='/static/redoc.standalone.js')

        @self._app.exception_handler(RequestValidationError)
        async def validation_exception_handler(request: Request, exc: RequestValidationError):
            logging.error(exc.errors())
            return await request_validation_exception_handler(request, exc)

    @abstractmethod
    def add_api_routes(self):
        pass

    def processing(self, data: Optional[Batch]) -> Optional[Batch]:
        self.add_api_routes()
        self._app.include_router(self._router)
        uvicorn.run(self._app, host=self._host, port=self._port)

        return None
