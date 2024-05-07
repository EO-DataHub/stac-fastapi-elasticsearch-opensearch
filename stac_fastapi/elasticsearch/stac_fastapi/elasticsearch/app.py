"""FastAPI application."""

import os
from typing import Any, Dict, Optional, cast

from fastapi import Depends
from fastapi.exceptions import HTTPException
from fastapi.openapi.models import OAuthFlows as OAuthFlowsModel
from fastapi.security import OAuth2
from fastapi.security.utils import get_authorization_scheme_param
from starlette.requests import Request
from starlette.status import HTTP_401_UNAUTHORIZED
from typing_extensions import Annotated, Doc

from stac_fastapi.api.app import StacApi
from stac_fastapi.api.models import create_get_request_model, create_post_request_model
from stac_fastapi.core.core import (
    BulkTransactionsClient,
    CoreClient,
    EsAsyncBaseFiltersClient,
    EsAsyncCollectionSearchClient,
    TransactionsClient,
)
from stac_fastapi.core.extensions import QueryExtension
from stac_fastapi.core.session import Session
from stac_fastapi.elasticsearch.config import ElasticsearchSettings
from stac_fastapi.elasticsearch.database_logic import (
    DatabaseLogic,
    create_collection_index,
    create_index_templates,
)
from stac_fastapi.extensions.core import (
    ContextExtension,
    FieldsExtension,
    FilterExtension,
    SortExtension,
    TokenPaginationExtension,
    TransactionExtension,
    CollectionSearchExtension,
)
from stac_fastapi.extensions.third_party import BulkTransactionExtension


class OAuth2ClientCredentials(OAuth2):
    """
    OAuth2 flow for authentication using a bearer token obtained with an OAuth2 code
    flow. An instance of it would be used as a dependency.
    """

    def __init__(
        self,
        tokenUrl: Annotated[
            str,
            Doc(
                """
                The URL to obtain the OAuth2 token.
                """
            ),
        ],
        refreshUrl: Annotated[
            Optional[str],
            Doc(
                """
                The URL to refresh the token and obtain a new one.
                """
            ),
        ] = None,
        scheme_name: Annotated[
            Optional[str],
            Doc(
                """
                Security scheme name.

                It will be included in the generated OpenAPI (e.g. visible at `/docs`).
                """
            ),
        ] = None,
        scopes: Annotated[
            Optional[Dict[str, str]],
            Doc(
                """
                The OAuth2 scopes that would be required by the *"path" operations* that
                use this dependency.
                """
            ),
        ] = None,
        description: Annotated[
            Optional[str],
            Doc(
                """
                Security scheme description.

                It will be included in the generated OpenAPI (e.g. visible at `/docs`).
                """
            ),
        ] = None,
        auto_error: Annotated[
            bool,
            Doc(
                """
                By default, if no HTTP Auhtorization header is provided, required for
                OAuth2 authentication, it will automatically cancel the request and
                send the client an error.

                If `auto_error` is set to `False`, when the HTTP Authorization header
                is not available, instead of erroring out, the dependency result will
                be `None`.

                This is useful when you want to have optional authentication.

                It is also useful when you want to have authentication that can be
                provided in one of multiple optional ways (for example, with OAuth2
                or in a cookie).
                """
            ),
        ] = True,
    ):
        if not scopes:
            scopes = {}
        flows = OAuthFlowsModel(
            clientCredentials=cast(
                Any,
                {
                    "tokenUrl": tokenUrl,
                    "refreshUrl": refreshUrl,
                    "scopes": scopes,
                },
            )
        )
        super().__init__(
            flows=flows,
            scheme_name=scheme_name,
            description=description,
            auto_error=auto_error,
        )

    async def __call__(self, request: Request) -> Optional[str]:
        authorization = request.headers.get("Authorization")
        scheme, param = get_authorization_scheme_param(authorization)
        if not authorization or scheme.lower() != "bearer":
            if self.auto_error:
                raise HTTPException(
                    status_code=HTTP_401_UNAUTHORIZED,
                    detail="Not authenticated",
                    headers={"WWW-Authenticate": "Bearer"},
                )
            else:
                return None  # pragma: nocover
        return param


settings = ElasticsearchSettings()
session = Session.create_from_settings(settings)

filter_extension = FilterExtension(client=EsAsyncBaseFiltersClient())
filter_extension.conformance_classes.append(
    "http://www.opengis.net/spec/cql2/1.0/conf/advanced-comparison-operators"
)

database_logic = DatabaseLogic()

collection_search_extension = CollectionSearchExtension(
    client=EsAsyncCollectionSearchClient(database_logic)
)
collection_search_extension.conformance_classes.append(
    "https://api.stacspec.org/v1.0.0-rc.1/collection-search"
)

extensions = [
    TransactionExtension(
        client=TransactionsClient(
            database=database_logic, session=session, settings=settings
        ),
        settings=settings,
    ),
    BulkTransactionExtension(
        client=BulkTransactionsClient(
            database=database_logic,
            session=session,
            settings=settings,
        )
    ),
    FieldsExtension(),
    QueryExtension(),
    SortExtension(),
    TokenPaginationExtension(),
    ContextExtension(),
    filter_extension,
    collection_search_extension,
]

post_request_model = create_post_request_model(extensions)

oauth2_scheme = OAuth2ClientCredentials(tokenUrl=os.getenv('TOKEN_URL'))

dependencies = [Depends(oauth2_scheme)]
routes = [
    {"path": "/collections", "method": "POST"},
    {"path": "/collections", "method": "PUT"},
    {"path": "/collections/{collectionId}", "method": "DELETE"},
    {"path": "/collections/{collectionId}/items", "method": "POST"},
    {"path": "/collections/{collectionId}/items/{itemId}", "method": "PUT"},
    {"path": "/collections/{collectionId}/items/{itemId}", "method": "DELETE"},
]

api = StacApi(
    title=os.getenv("STAC_FASTAPI_TITLE", "stac-fastapi-elasticsearch"),
    description=os.getenv("STAC_FASTAPI_DESCRIPTION", "stac-fastapi-elasticsearch"),
    api_version=os.getenv("STAC_FASTAPI_VERSION", "2.1"),
    settings=settings,
    extensions=extensions,
    client=CoreClient(
        database=database_logic, session=session, post_request_model=post_request_model
    ),
    search_get_request_model=create_get_request_model(extensions),
    search_post_request_model=post_request_model,
    route_dependencies=[(routes, dependencies)],
)
app = api.app
app.root_path = os.getenv("STAC_FASTAPI_ROOT_PATH", "")


@app.on_event("startup")
async def _startup_event() -> None:
    await create_index_templates()
    await create_collection_index()


def run() -> None:
    """Run app from command line using uvicorn if available."""
    try:
        import uvicorn

        uvicorn.run(
            "stac_fastapi.elasticsearch.app:app",
            host=settings.app_host,
            port=settings.app_port,
            log_level="info",
            reload=settings.reload,
        )
    except ImportError:
        raise RuntimeError("Uvicorn must be installed in order to use command")


if __name__ == "__main__":
    run()


def create_handler(app):
    """Create a handler to use with AWS Lambda if mangum available."""
    try:
        from mangum import Mangum

        return Mangum(app)
    except ImportError:
        return None


handler = create_handler(app)
