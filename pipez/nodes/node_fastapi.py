import importlib.resources
from abc import ABC, abstractmethod
from typing import Optional

import uvicorn
from fastapi import APIRouter, FastAPI
from fastapi.openapi.docs import get_swagger_ui_html, get_swagger_ui_oauth2_redirect_html
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
        self._app = FastAPI(docs_url=None)
        self._router = APIRouter()

        self._app.mount(path='/static',
                        app=StaticFiles(directory=importlib.resources.files('pipez.resources') / 'swagger_ui'),
                        name='static')

        @self._app.get('/docs', include_in_schema=False)
        async def custom_swagger_ui_html():
            return get_swagger_ui_html(openapi_url=self._app.openapi_url,
                                       title=self._app.title,
                                       swagger_js_url='/static/swagger-ui-bundle.js',
                                       swagger_css_url='/static/swagger-ui.css',
                                       oauth2_redirect_url=self._app.swagger_ui_oauth2_redirect_url)

        @self._app.get(self._app.swagger_ui_oauth2_redirect_url, include_in_schema=False)
        async def swagger_ui_redirect():
            return get_swagger_ui_oauth2_redirect_html()

    @abstractmethod
    def add_api_routes(self):
        pass

    def processing(self, data: Optional[Batch]) -> Optional[Batch]:
        self.add_api_routes()
        self._app.include_router(self._router)
        uvicorn.run(self._app, host=self._host, port=self._port)

        return None
