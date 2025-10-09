import importlib.resources
from abc import ABC
from typing import Optional

import uvicorn
from fastapi import FastAPI
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
        self.host = host
        self.port = port
        self.app = FastAPI(docs_url=None)

        self.app.mount('/static',
                       StaticFiles(directory=importlib.resources.files('pipez.resources') / 'fastapi'),
                       name='static')

        @self.app.get('/docs', include_in_schema=False)
        async def custom_swagger_ui_html():
            return get_swagger_ui_html(openapi_url=self.app.openapi_url,
                                       title=self.app.title,
                                       oauth2_redirect_url=self.app.swagger_ui_oauth2_redirect_url,
                                       swagger_js_url='/static/swagger-ui-bundle.js',
                                       swagger_css_url='/static/swagger-ui.css')

        @self.app.get(self.app.swagger_ui_oauth2_redirect_url, include_in_schema=False)
        async def swagger_ui_redirect():
            return get_swagger_ui_oauth2_redirect_html()

    def processing(self, data: Optional[Batch]) -> Optional[Batch]:
        uvicorn.run(app=self.app,
                    host=self.host,
                    port=self.port)

        return None
