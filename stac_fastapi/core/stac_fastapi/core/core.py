"""Core client."""

import logging
from datetime import datetime as datetime_type
from datetime import timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Type, Union
from urllib.parse import unquote_plus, urljoin

import attr
import orjson
from fastapi import HTTPException, Request
from overrides import overrides
from pydantic import ValidationError
from pygeofilter.backends.cql2_json import to_cql2
from pygeofilter.parsers.cql2_text import parse as parse_cql2_text
from stac_pydantic import Catalog, Collection, Item, ItemCollection
from stac_pydantic.links import Relations
from stac_pydantic.shared import BBox, MimeTypes
from stac_pydantic.version import STAC_VERSION

from stac_fastapi.core.base_database_logic import BaseDatabaseLogic
from stac_fastapi.core.base_settings import ApiBaseSettings
from stac_fastapi.core.models.links import PagingLinks
from stac_fastapi.core.serializers import CatalogSerializer, CollectionSerializer, ItemSerializer
from stac_fastapi.core.session import Session
from stac_fastapi.core.utilities import filter_fields
from stac_fastapi.extensions.core.filter.client import AsyncBaseFiltersClient
from stac_fastapi.extensions.core.filter.request import FilterLang
from stac_fastapi.extensions.core.collection_search.client import AsyncBaseCollectionSearchClient
from stac_fastapi.extensions.core.collection_search.request import BaseCollectionSearchPostRequest
from stac_fastapi.extensions.third_party.bulk_transactions import (
    BaseBulkTransactionsClient,
    BulkTransactionMethod,
    Items,
)
from stac_fastapi.types import stac as stac_types
from stac_fastapi.types.access_policy import AccessPolicy
from stac_fastapi.types.conformance import BASE_CONFORMANCE_CLASSES
from stac_fastapi.types.core import AsyncBaseCoreClient, AsyncBaseTransactionsClient
from stac_fastapi.types.extension import ApiExtension
from stac_fastapi.types.requests import get_base_url
from stac_fastapi.types.rfc3339 import DateTimeType
from stac_fastapi.types.search import BaseSearchPostRequest

# Get the logger for this module
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # Set the logging level to INFO for this module

# Create a console handler and set the level to INFO
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Create a formatter and set it for the handler
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(console_handler)



NumType = Union[float, int]


@attr.s
class CoreClient(AsyncBaseCoreClient):
    """Client for core endpoints defined by the STAC specification.

    This class is a implementation of `AsyncBaseCoreClient` that implements the core endpoints
    defined by the STAC specification. It uses the `DatabaseLogic` class to interact with the
    database, and `ItemSerializer`, `CollectionSerializer` and `CatalogSerializer` to convert between STAC objects and
    database records.

    Attributes:
        session (Session): A requests session instance to be used for all HTTP requests.
        item_serializer (Type[serializers.ItemSerializer]): A serializer class to be used to convert
            between STAC items and database records.
        collection_serializer (Type[serializers.CollectionSerializer]): A serializer class to be
            used to convert between STAC collections and database records.
        catalog_serializer (Type[serializers.CatalogSerializer]): A serializer class to be used to
            convert between STAC catalogs and database records.
        database (DatabaseLogic): An instance of the `DatabaseLogic` class that is used to interact
            with the database.
    """

    database: BaseDatabaseLogic = attr.ib()
    base_conformance_classes: List[str] = attr.ib(
        factory=lambda: BASE_CONFORMANCE_CLASSES
    )
    extensions: List[ApiExtension] = attr.ib(default=attr.Factory(list))

    session: Session = attr.ib(default=attr.Factory(Session.create_from_env))
    item_serializer: Type[ItemSerializer] = attr.ib(default=ItemSerializer)
    collection_serializer: Type[CollectionSerializer] = attr.ib(
        default=CollectionSerializer
    )
    catalog_serializer: Type[CatalogSerializer] = attr.ib(default=CatalogSerializer)
    post_request_model = attr.ib(default=BaseSearchPostRequest)
    stac_version: str = attr.ib(default=STAC_VERSION)
    landing_page_id: str = attr.ib(default="stac-fastapi")
    title: str = attr.ib(default="stac-fastapi")
    description: str = attr.ib(default="stac-fastapi")

    def _landing_page(
        self,
        base_url: str,
        conformance_classes: List[str],
        extension_schemas: List[str],
    ) -> stac_types.LandingPage:
        landing_page = stac_types.LandingPage(
            type="Catalog",
            id=self.landing_page_id,
            title=self.title,
            description=self.description,
            stac_version=self.stac_version,
            conformsTo=conformance_classes,
            links=[
                {
                    "rel": Relations.self.value,
                    "type": MimeTypes.json,
                    "href": base_url,
                },
                {
                    "rel": Relations.root.value,
                    "type": MimeTypes.json,
                    "href": base_url,
                },
                {
                    "rel": "data",
                    "type": MimeTypes.json,
                    "href": urljoin(base_url, "collections"),
                },
                {
                    "rel": Relations.conformance.value,
                    "type": MimeTypes.json,
                    "title": "STAC/WFS3 conformance classes implemented by this server",
                    "href": urljoin(base_url, "conformance"),
                },
                {
                    "rel": Relations.search.value,
                    "type": MimeTypes.geojson,
                    "title": "STAC search",
                    "href": urljoin(base_url, "search"),
                    "method": "GET",
                },
                {
                    "rel": Relations.search.value,
                    "type": MimeTypes.geojson,
                    "title": "STAC search",
                    "href": urljoin(base_url, "search"),
                    "method": "POST",
                },
            ],
            stac_extensions=extension_schemas,
        )
        return landing_page

    async def landing_page(self, auth_headers: dict, **kwargs) -> stac_types.LandingPage:
        """Landing page.

        Called with `GET /`.

        Returns:
            API landing page, serving as an entry point to the API.
        """
        logger.info("Getting landing page")
        request: Request = kwargs["request"]
        workspaces = auth_headers.get("X-Workspaces", [])
        base_url = get_base_url(request)
        landing_page = self._landing_page(
            base_url=base_url,
            conformance_classes=self.conformance_classes(),
            extension_schemas=[],
        )

        if self.extension_is_enabled("FilterExtension"):
            landing_page["links"].append(
                {
                    # TODO: replace this with Relations.queryables.value,
                    "rel": "queryables",
                    # TODO: replace this with MimeTypes.jsonschema,
                    "type": "application/schema+json",
                    "title": "Queryables",
                    "href": urljoin(base_url, "queryables"),
                }
            )

        if self.extension_is_enabled("AggregationExtension"):
            landing_page["links"].extend(
                [
                    {
                        "rel": "aggregate",
                        "type": "application/json",
                        "title": "Aggregate",
                        "href": urljoin(base_url, "aggregate"),
                    },
                    {
                        "rel": "aggregations",
                        "type": "application/json",
                        "title": "Aggregations",
                        "href": urljoin(base_url, "aggregations"),
                    },
                ]
            )

        catalogs = await self.get_all_sub_catalogs(cat_path="", workspaces=workspaces)
        for catalog in catalogs["catalogs"]:
            landing_page["links"].append(
                {
                    "rel": Relations.child.value,
                    "type": MimeTypes.json.value,
                    "title": catalog[1],
                    "href": urljoin(base_url, f"catalogs/{catalog[0]}"),
                }
            )

        # Add OpenAPI URL
        landing_page["links"].append(
            {
                "rel": "service-desc",
                "type": "application/vnd.oai.openapi+json;version=3.0",
                "title": "OpenAPI service description",
                "href": urljoin(
                    str(request.base_url), request.app.openapi_url.lstrip("/")
                ),
            }
        )

        # Add human readable service-doc
        landing_page["links"].append(
            {
                "rel": "service-doc",
                "type": "text/html",
                "title": "OpenAPI service documentation",
                "href": urljoin(
                    str(request.base_url), request.app.docs_url.lstrip("/")
                ),
            }
        )

        return landing_page

    async def all_collections(
        self, 
        auth_headers: dict, 
        cat_path: str = None, 
        bbox: Optional[BBox] = None,
        datetime: Optional[DateTimeType] = None,
        limit: Optional[int] = 10,
        token: Optional[str] = None,
        q: Optional[List[str]] = None,
        **kwargs) -> stac_types.Collections:
        """Read all collections from the database.

        Args:
            auth_headers (dict): The authentication headers.
            cat_path (str): The path of the parent catalog containing the collections.
            bbox (Optional[BBox]): The bounding box to filter collections by.
            datetime (Optional[DateTimeType]): The datetime range to filter collections by.
            limit (Optional[int]): The maximum number of collections to return. The default value is 10.
            token (Optional[str]): A token used for pagination.
            q (Optional[List[str]]): A list of free text queries to filter the collections by.
            **kwargs: Keyword arguments from the request.

        Returns:
            A Collections object containing all the collections in the database and links to various resources.
        """
        logger.info("Getting all collections")
        request = kwargs["request"]
        base_url = str(request.base_url)
        workspaces = auth_headers.get("X-Workspaces", [])
        user_is_authenticated = auth_headers.get("X-Authenticated", False)
        token = request.query_params.get("token")
        search = self.database.make_search()

        if cat_path:
            await self.database.find_catalog(cat_path=cat_path, workspaces=workspaces, user_is_authenticated=user_is_authenticated)

        # Apply user access filter
        search = self.database.apply_access_filter(
            search=search, workspaces=workspaces
        )

        if cat_path:
            search = self.database.apply_recursive_catalogs_filter(
                    search=search, catalog_path=cat_path
                )

        if datetime:
            datetime_search = CoreClient._return_date(datetime)
            search = self.database.apply_datetime_collections_filter(
                search=search, datetime_search=datetime_search
            )

        if bbox:
            if len(bbox) == 6:
                bbox = [bbox[0], bbox[1], bbox[3], bbox[4]]

            search = self.database.apply_bbox_collections_filter(
                search=search, bbox=bbox
            )

        if q:
            search = self.database.apply_keyword_collections_filter(search=search, q=q)

        collections, maybe_count, next_token = await self.database.execute_collection_search(
            search=search,
            limit=limit,
            token=token,  # type: ignore
            sort=None,
        )

        collections = [
            self.collection_serializer.db_to_stac(collection, request=request) for collection in collections
        ]

        if not cat_path:
            parent_href_url = ""
            collections_href_url = "collections"
        else:
            parent_href_url = f"catalogs/{cat_path}"
            collections_href_url = f"catalogs/{cat_path}/collections"

        links = [
            {"rel": Relations.root.value, "type": MimeTypes.json, "href": base_url},
            {"rel": Relations.parent.value, "type": MimeTypes.json, "href": urljoin(base_url, parent_href_url)},
            {
                "rel": Relations.self.value,
                "type": MimeTypes.json,
                "href": urljoin(base_url, collections_href_url),
            },
        ]

        if next_token:
            next_link = PagingLinks(next=next_token, request=request).link_next()
            links.append(next_link)

        return stac_types.Collections(
            collections=collections,
            links=links,
            numReturned=len(collections),
            numMatched=maybe_count,
        )


    async def get_collection(
        self, cat_path: str, collection_id: str, auth_headers: dict, **kwargs
    ) -> stac_types.Collection:
        """Get a collection from the database by its id.

        Args:
            cat_path: The path of the parent catalog containing the specified collection.
            collection_id (str): The id of the collection to retrieve.
            auth_headers (dict): The authentication headers.
            kwargs: Additional keyword arguments passed to the API call.

        Returns:
            Collection: A `Collection` object representing the requested collection.

        Raises:
            NotFoundError: If the collection with the given id cannot be found in the database.
        """
        logger.info(f"Getting collection {collection_id}")
        request = kwargs["request"]
        workspaces = auth_headers.get("X-Workspaces", [])
        user_is_authenticated = auth_headers.get("X-Authenticated", False)
        collection = await self.database.find_collection(cat_path=cat_path, collection_id=collection_id, workspaces=workspaces, user_is_authenticated=user_is_authenticated)
        return self.collection_serializer.db_to_stac(
            collection=collection,
            request=request,
            extensions=[type(ext).__name__ for ext in self.extensions],
        )
    
    async def all_catalogs(self, auth_headers: dict, cat_path: str = None, **kwargs) -> stac_types.Catalogs:
        """Read all catalogs from the database.

        Args:
            auth_headers (dict): The authentication headers.
            cat_path (str): The path of the parent catalog containing the catalogs.
            root_only (bool): If True, only the top-level catalogs will be returned.
            **kwargs: Keyword arguments from the request.

        Returns:
            A Catalogs object containing all the catalogs in the database and links to various resources.
        """
        logger.info("Getting all catalogs")
        request = kwargs["request"]
        workspaces = auth_headers.get("X-Workspaces", [])
        base_url = str(request.base_url)
        limit = int(request.query_params.get("limit", 10))
        token = request.query_params.get("token")

        # We want all the top-level catalogs returned
        if cat_path == "":
            limit = 10_000

        catalogs, maybe_count, next_token = await self.database.get_all_catalogs(
            cat_path=cat_path, token=token, limit=limit, request=request, workspaces=workspaces, conformance_classes=self.conformance_classes(),
        )

        if not cat_path:
            parent_href_url = ""
            catalogs_href_url = "catalogs"
        else:
            parent_href_url = f"catalogs/{cat_path}"
            catalogs_href_url = f"catalogs/{cat_path}/catalogs"

        links = [
            {"rel": Relations.root.value, "type": MimeTypes.json, "href": base_url},
            {"rel": Relations.parent.value, "type": MimeTypes.json, "href": urljoin(base_url, parent_href_url)},
            {
                "rel": Relations.self.value,
                "type": MimeTypes.json,
                "href": urljoin(base_url, catalogs_href_url),
            },
        ]

        if next_token:
            next_link = PagingLinks(next=next_token, request=request).link_next()
            links.append(next_link)

        return stac_types.Catalogs(
            catalogs=catalogs, 
            links=links,
            numReturned=len(catalogs),
            numMatched=maybe_count
        )

    async def get_catalog(
        self, catalog_id: str, auth_headers: dict, cat_path: Optional[str]=None,  **kwargs
    ) -> stac_types.Catalog:
        """Get a catalog from the database by its id.

        Args:
            cat_path (str): The path of the catalog to retrieve.
            kwargs: Additional keyword arguments passed to the API call.

        Returns:
            Catalog: A `Catalog` object representing the requested catalog.

        Raises:
            NotFoundError: If the catalog with the given path cannot be found in the database.
        """
        logger.info(f"Getting catalog {catalog_id}")
        request = kwargs["request"]
        workspaces = auth_headers.get("X-Workspaces", [])
        user_is_authenticated = auth_headers.get("X-Authenticated", False)
        if cat_path:
            cat_path = f"{cat_path}/catalogs/{catalog_id}"
        else:
            cat_path = catalog_id
        catalog = await self.database.find_catalog(cat_path=cat_path, workspaces=workspaces, user_is_authenticated=user_is_authenticated)
        sub_catalogs = await self.database.get_all_sub_catalogs(cat_path=cat_path, workspaces=workspaces)
        sub_collections = await self.database.get_all_sub_collections(cat_path=cat_path, workspaces=workspaces)
        return self.catalog_serializer.db_to_stac(
            catalog=catalog,
            sub_catalogs=sub_catalogs,
            sub_collections=sub_collections,
            request=request,
            conformance_classes=self.conformance_classes(),
            extensions=[type(ext).__name__ for ext in self.extensions],
        )

    async def item_collection(
        self,
        cat_path: str,
        collection_id: str,
        auth_headers: dict,
        bbox: Optional[BBox] = None,
        datetime: Optional[DateTimeType] = None,
        limit: Optional[int] = 10,
        token: Optional[str] = None,
        **kwargs,
    ) -> stac_types.ItemCollection:
        """Read items from a specific collection in the database.

        Args:
            cat_path (str): The path of the parent catalog containing the collection.
            collection_id (str): The identifier of the collection to read items from.
            auth_headers (dict): The authentication headers.
            bbox (Optional[BBox]): The bounding box to filter items by.
            datetime (Optional[DateTimeType]): The datetime range to filter items by.
            limit (int): The maximum number of items to return. The default value is 10.
            token (str): A token used for pagination.
            request (Request): The incoming request.

        Returns:
            ItemCollection: An `ItemCollection` object containing the items from the specified collection that meet
                the filter criteria and links to various resources.

        Raises:
            HTTPException: If the specified collection is not found.
            Exception: If any error occurs while reading the items from the database.
        """
        logger.info(f"Getting items from collection {collection_id}")
        request: Request = kwargs["request"]
        workspaces = auth_headers.get("X-Workspaces", [])
        token = request.query_params.get("token")
        base_url = str(request.base_url)

        collection = await self.get_collection(
            cat_path=cat_path, collection_id=collection_id, request=request, auth_headers=auth_headers
        )
        collection_id = collection.get("id")
        if collection_id is None:
            raise HTTPException(status_code=404, detail="Collection not found")

        search = self.database.make_search()
        search = self.database.apply_collections_filter(
            search=search, collection_ids=[collection_id]
        )
        search = self.database.apply_recursive_catalogs_filter(
            search=search, catalog_path=cat_path
        )
        search = self.database.apply_access_filter(
            search=search, workspaces=workspaces
        )

        if datetime:
            datetime_search = self._return_date(datetime)
            search = self.database.apply_datetime_filter(
                search=search, datetime_search=datetime_search
            )

        if bbox:
            bbox = [float(x) for x in bbox]
            if len(bbox) == 6:
                bbox = [bbox[0], bbox[1], bbox[3], bbox[4]]

            search = self.database.apply_bbox_filter(search=search, bbox=bbox)

        items, maybe_count, next_token = await self.database.execute_search(
            search=search,
            limit=limit,
            sort=None,
            token=token,  # type: ignore
            collection_ids=[collection_id],
        )

        items = [
            self.item_serializer.db_to_stac(item, base_url=base_url) for item in items
        ]

        links = await PagingLinks(request=request, next=next_token).get_links()

        return stac_types.ItemCollection(
            type="FeatureCollection",
            features=items,
            links=links,
            numReturned=len(items),
            numMatched=maybe_count,
        )

    async def get_item(
        self, cat_path: str, item_id: str, collection_id: str, auth_headers: dict, **kwargs
    ) -> stac_types.Item:
        """Get an item from the database based on its id and collection id.

        Args:
            cat_path (str): The path of the parent catalog containing the collection.
            collection_id (str): The ID of the collection the item belongs to.
            item_id (str): The ID of the item to be retrieved.
            auth_headers (dict): The authentication headers.

        Returns:
            Item: An `Item` object representing the requested item.

        Raises:
            Exception: If any error occurs while getting the item from the database.
            NotFoundError: If the item does not exist in the specified collection.
        """
        logger.info(f"Getting item {item_id} from collection {collection_id}")
        base_url = str(kwargs["request"].base_url)
        workspaces = auth_headers.get("X-Workspaces", [])
        user_is_authenticated = auth_headers.get("X-Authenticated", False)
        item = await self.database.get_one_item(
            cat_path=cat_path, item_id=item_id, collection_id=collection_id, workspaces=workspaces, user_is_authenticated=user_is_authenticated
        )
        return self.item_serializer.db_to_stac(item, base_url)

    @staticmethod
    def _return_date(
        interval: Optional[Union[DateTimeType, str]]
    ) -> Dict[str, Optional[str]]:
        """
        Convert a date interval.

        (which may be a datetime, a tuple of one or two datetimes a string
        representing a datetime or range, or None) into a dictionary for filtering
        search results with Elasticsearch.

        This function ensures the output dictionary contains 'gte' and 'lte' keys,
        even if they are set to None, to prevent KeyError in the consuming logic.

        Args:
            interval (Optional[Union[DateTimeType, str]]): The date interval, which might be a single datetime,
                a tuple with one or two datetimes, a string, or None.

        Returns:
            dict: A dictionary representing the date interval for use in filtering search results,
                always containing 'gte' and 'lte' keys.
        """
        result: Dict[str, Optional[str]] = {"gte": None, "lte": None}

        if interval is None:
            return result

        if isinstance(interval, str):
            if "/" in interval:
                parts = interval.split("/")
                result["gte"] = parts[0] if parts[0] != ".." else None
                result["lte"] = (
                    parts[1] if len(parts) > 1 and parts[1] != ".." else None
                )
            else:
                converted_time = interval if interval != ".." else None
                result["gte"] = result["lte"] = converted_time
            return result

        if isinstance(interval, datetime_type):
            datetime_iso = interval.isoformat()
            result["gte"] = result["lte"] = datetime_iso
        elif isinstance(interval, tuple):
            start, end = interval
            # Ensure datetimes are converted to UTC and formatted with 'Z'
            if start:
                result["gte"] = start.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            if end:
                result["lte"] = end.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

        return result

    def _format_datetime_range(self, date_tuple: DateTimeType) -> str:
        """
        Convert a tuple of datetime objects or None into a formatted string for API requests.

        Args:
            date_tuple (tuple): A tuple containing two elements, each can be a datetime object or None.

        Returns:
            str: A string formatted as 'YYYY-MM-DDTHH:MM:SS.sssZ/YYYY-MM-DDTHH:MM:SS.sssZ', with '..' used if any element is None.
        """

        def format_datetime(dt):
            """Format a single datetime object to the ISO8601 extended format with 'Z'."""
            return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z" if dt else ".."

        start, end = date_tuple
        return f"{format_datetime(start)}/{format_datetime(end)}"

    async def get_search(
        self,
        request: Request,
        auth_headers: dict,
        cat_path: str = None,
        collections: Optional[List[str]] = None,
        ids: Optional[List[str]] = None,
        bbox: Optional[BBox] = None,
        datetime: Optional[DateTimeType] = None,
        limit: Optional[int] = 10,
        query: Optional[str] = None,
        token: Optional[str] = None,
        fields: Optional[List[str]] = None,
        sortby: Optional[str] = None,
        q: Optional[List[str]] = None,
        intersects: Optional[str] = None,
        filter: Optional[str] = None,
        filter_lang: Optional[str] = None,
        **kwargs,
    ) -> stac_types.ItemCollection:
        """Get search results from the database.

        Args:
            request (Request): The incoming request.
            auth_headers (dict): The authentication headers.
            cat_path (str): The path of the parent catalog containing the collections.
            collections (Optional[List[str]]): List of collection IDs to search in.
            ids (Optional[List[str]]): List of item IDs to search for.
            bbox (Optional[BBox]): Bounding box to search in.
            datetime (Optional[DateTimeType]): Filter items based on the datetime field.
            limit (Optional[int]): Maximum number of results to return.
            query (Optional[str]): Query string to filter the results.
            token (Optional[str]): Access token to use when searching the catalog.
            fields (Optional[List[str]]): Fields to include or exclude from the results.
            sortby (Optional[str]): Sorting options for the results.
            q (Optional[List[str]]): Free text query to filter the results.
            intersects (Optional[str]): GeoJSON geometry to search in.
            kwargs: Additional parameters to be passed to the API.

        Returns:
            ItemCollection: Collection of `Item` objects representing the search results.

        Raises:
            HTTPException: If any error occurs while searching the catalog.
        """

        logger.info("Getting search results")
        base_args = {
            "collections": collections,
            "ids": ids,
            "bbox": bbox,
            "limit": limit,
            "token": token,
            "query": orjson.loads(query) if query else query,
            "q": q,
        }

        if datetime:
            base_args["datetime"] = self._format_datetime_range(datetime)

        if intersects:
            base_args["intersects"] = orjson.loads(unquote_plus(intersects))

        if sortby:
            base_args["sortby"] = [
                {"field": sort[1:], "direction": "desc" if sort[0] == "-" else "asc"}
                for sort in sortby
            ]

        if filter:
            base_args["filter-lang"] = "cql2-json"
            base_args["filter"] = orjson.loads(
                unquote_plus(filter)
                if filter_lang == "cql2-json"
                else to_cql2(parse_cql2_text(filter))
            )

        if fields:
            includes, excludes = set(), set()
            for field in fields:
                if field[0] == "-":
                    excludes.add(field[1:])
                else:
                    includes.add(field[1:] if field[0] in "+ " else field)
            base_args["fields"] = {"include": includes, "exclude": excludes}

        # Do the request
        try:
            search_request = self.post_request_model(**base_args)
        except ValidationError as e:
            raise HTTPException(
                status_code=400, detail=f"Invalid parameters provided: {e}"
            )
        resp = await self.post_search(search_request=search_request, request=request, cat_path=cat_path, auth_headers=auth_headers)

        return resp

    async def post_search(
        self, search_request: BaseSearchPostRequest, request: Request, auth_headers: dict, cat_path: str = None, **kwargs
    ) -> stac_types.ItemCollection:
        """
        Perform a POST search on the catalog.

        Args:
            search_request (BaseSearchPostRequest): Request object that includes the parameters for the search.
            request (Request): The incoming request.
            auth_headers (dict): The authentication headers.
            cat_path (str): The path of the parent catalog containing the collections.
            kwargs: Keyword arguments passed to the function.

        Returns:
            ItemCollection: A collection of items matching the search criteria.

        Raises:
            HTTPException: If there is an error with the cql2_json filter.
        """
        logger.info("Performing POST search")
        workspaces = auth_headers.get("X-Workspaces", [])
        user_is_authenticated = auth_headers.get("X-Authenticated", False)

        if cat_path:
            await self.database.find_catalog(cat_path=cat_path, workspaces=workspaces, user_is_authenticated=user_is_authenticated)

        base_url = str(request.base_url)

        search = self.database.make_search()

        # Apply user access filter
        search = self.database.apply_access_filter(
            search=search, workspaces=workspaces
        )

        if search_request.ids:
            search = self.database.apply_ids_filter(
                search=search, item_ids=search_request.ids
            )

        if cat_path:
            search = self.database.apply_recursive_catalogs_filter(
                    search=search, catalog_path=cat_path
                )

        if search_request.collections:
            search = self.database.apply_collections_filter(
                search=search, collection_ids=search_request.collections
            )

        if search_request.datetime:
            datetime_search = self._return_date(search_request.datetime)
            search = self.database.apply_datetime_filter(
                search=search, datetime_search=datetime_search
            )

        if search_request.bbox:
            bbox = search_request.bbox
            if len(bbox) == 6:
                bbox = [bbox[0], bbox[1], bbox[3], bbox[4]]

            search = self.database.apply_bbox_filter(search=search, bbox=bbox)

        if search_request.intersects:
            search = self.database.apply_intersects_filter(
                search=search, intersects=search_request.intersects
            )

        if search_request.query:
            for field_name, expr in search_request.query.items():
                field = "properties__" + field_name
                for op, value in expr.items():
                    # Convert enum to string
                    operator = op.value if isinstance(op, Enum) else op
                    search = self.database.apply_stacql_filter(
                        search=search, op=operator, field=field, value=value
                    )

        # only cql2_json is supported here
        if hasattr(search_request, "filter"):
            cql2_filter = getattr(search_request, "filter", None)
            try:
                search = self.database.apply_cql2_filter(search, cql2_filter)
            except Exception as e:
                raise HTTPException(
                    status_code=400, detail=f"Error with cql2_json filter: {e}"
                )

        if hasattr(search_request, "q"):
            free_text_queries = getattr(search_request, "q", None)
            try:
                search = self.database.apply_free_text_filter(search, free_text_queries)
            except Exception as e:
                raise HTTPException(
                    status_code=400, detail=f"Error with free text query: {e}"
                )

        sort = None
        if search_request.sortby:
            sort = self.database.populate_sort(search_request.sortby)

        limit = 10
        if search_request.limit:
            limit = search_request.limit

        items, maybe_count, next_token = await self.database.execute_search(
            search=search,
            limit=limit,
            token=search_request.token,  # type: ignore
            sort=sort,
            collection_ids=search_request.collections,
        )

        fields = (
            getattr(search_request, "fields", None)
            if self.extension_is_enabled("FieldsExtension")
            else None
        )
        include: Set[str] = fields.include if fields and fields.include else set()
        exclude: Set[str] = fields.exclude if fields and fields.exclude else set()

        items = [
            filter_fields(
                self.item_serializer.db_to_stac(item, base_url=base_url),
                include,
                exclude,
            )
            for item in items
        ]

        if not cat_path:
            parent_href_url = ""
            search_href_url = "search"
        else:
            parent_href_url = f"catalogs/{cat_path}"
            search_href_url = f"catalogs/{cat_path}/search"

        links = [
            {"rel": Relations.root.value, "type": MimeTypes.json, "href": base_url},
            {"rel": Relations.parent.value, "type": MimeTypes.json, "href": urljoin(base_url, parent_href_url)},
            {
                "rel": Relations.self.value,
                "type": MimeTypes.json,
                "href": urljoin(base_url, search_href_url),
            },
        ]

        if next_token:
            next_link = PagingLinks(next=next_token, request=request).link_next()
            links.append(next_link)

        return stac_types.ItemCollection(
            type="FeatureCollection",
            features=items,
            links=links,
            numReturned=len(items),
            numMatched=maybe_count,
        )


@attr.s
class TransactionsClient(AsyncBaseTransactionsClient):
    """Transactions extension specific CRUD operations."""

    database: BaseDatabaseLogic = attr.ib()
    settings: ApiBaseSettings = attr.ib()
    session: Session = attr.ib(default=attr.Factory(Session.create_from_env))

    @overrides
    async def create_item(
        self, cat_path: str, collection_id: str, item: Union[Item, ItemCollection], workspace: str, **kwargs
    ) -> Optional[stac_types.Item]:
        """Create an item in the collection.

        Args:
            cat_path (str): The path of the parent catalog containing the collection.
            collection_id (str): The id of the collection to add the item to.
            item (stac_types.Item): The item to be added to the collection.
            workspace (str): The workspace making the request.
            kwargs: Additional keyword arguments.

        Returns:
            stac_types.Item: The created item.

        Raises:
            NotFound: If the specified collection is not found in the database.
            ConflictError: If the item in the specified collection already exists.

        """
        item = item.model_dump(mode="json")
        base_url = str(kwargs["request"].base_url)

        # If a feature collection is posted
        if item["type"] == "FeatureCollection":
            bulk_client = BulkTransactionsClient(
                database=self.database, settings=self.settings
            )
            processed_items = [
                bulk_client.preprocess_item(item, base_url, BulkTransactionMethod.INSERT) for item in item["features"]  # type: ignore
            ]

            await self.database.bulk_async(
                cat_path, collection_id, processed_items, refresh=kwargs.get("refresh", False)
            )

            return None
        else:
            if collection_id != item["collection"]:
                raise HTTPException(status_code=400, detail="Collection ID in path does not match collection ID in item")
            await self.database.create_item(cat_path, collection_id, item, workspace, refresh=kwargs.get("refresh", False))
            return ItemSerializer.db_to_stac(item, base_url)

    @overrides
    async def update_item(
        self, cat_path: str, collection_id: str, item_id: str, item: Item, workspace: str, **kwargs
    ) -> stac_types.Item:
        """Update an item in the collection.

        Args:
            cat_path (str): The path of the parent catalog containing the collection.
            collection_id (str): The ID of the collection the item belongs to.
            item_id (str): The ID of the item to be updated.
            item (stac_types.Item): The new item data.
            workspace (str): The workspace making the request.
            kwargs: Other optional arguments, including the request object.

        Returns:
            stac_types.Item: The updated item object.

        Raises:
            NotFound: If the specified collection is not found in the database.

        """
        item = item.model_dump(mode="json")
        base_url = str(kwargs["request"].base_url)
        now = datetime_type.now(timezone.utc).isoformat().replace("+00:00", "Z")
        item["properties"]["updated"] = now

        await self.database.check_collection_exists(cat_path=cat_path, collection_id=collection_id)
        await self.delete_item(cat_path=cat_path, item_id=item_id, collection_id=collection_id, workspace=workspace)
        await self.create_item(cat_path=cat_path, collection_id=collection_id, item=Item(**item), workspace=workspace, **kwargs)

        return ItemSerializer.db_to_stac(item, base_url)

    @overrides
    async def delete_item(
        self, cat_path: str, item_id: str, collection_id: str, workspace: str, **kwargs
    ) -> Optional[stac_types.Item]:
        """Delete an item from a collection.

        Args:
            cat_path (str): The path of the parent catalog containing the collection.
            item_id (str): The identifier of the item to delete.
            collection_id (str): The identifier of the collection that contains the item.
            workspace (str): The workspace making the request.
            kwargs: Additional keyword arguments.

        Returns:
            Optional[stac_types.Item]: The deleted item, or `None` if the item was successfully deleted.
        """
        await self.database.delete_item(cat_path=cat_path, item_id=item_id, collection_id=collection_id, workspace=workspace)
        return None

    @overrides
    async def create_collection(
        self, cat_path: str, collection: Collection, workspace: str, **kwargs
    ) -> stac_types.Collection:
        """Create a new collection in the database.

        Args:
            cat_path (str): The path of the parent catalog containing the collections.
            collection (stac_types.Collection): The collection to be created.
            workspace (str): The workspace making the request.
            kwargs: Additional keyword arguments.

        Returns:
            stac_types.Collection: The created collection object.

        Raises:
            ConflictError: If the collection already exists.
        """
        collection = collection.model_dump(mode="json")
        request = kwargs["request"]
        collection = self.database.collection_serializer.stac_to_db(collection, request)
        await self.database.create_collection(cat_path=cat_path, collection=collection, workspace=workspace)
        return CollectionSerializer.db_to_stac(
            collection,
            request,
            extensions=[type(ext).__name__ for ext in self.database.extensions],
        )

    @overrides
    async def update_collection(
        self, cat_path: str, collection_id: str, collection: Collection, workspace: str, **kwargs
    ) -> stac_types.Collection:
        """
        Update a collection.

        This method updates an existing collection in the database by first finding
        the collection by the id given in the keyword argument `collection_id`.
        If no `collection_id` is given the id of the given collection object is used.
        If the object and keyword collection ids don't match the sub items
        collection id is updated else the items are left unchanged.
        The updated collection is then returned.

        Args:
            cat_path: The path of the parent catalog containing the collection.
            collection_id: id of the existing collection to be updated
            collection: A STAC collection that needs to be updated.
            workspace: The workspace making the request.
            kwargs: Additional keyword arguments.

        Returns:
            A STAC collection that has been updated in the database.

        """
        collection = collection.model_dump(mode="json")

        request = kwargs["request"]

        collection = self.database.collection_serializer.stac_to_db(collection, request)
        await self.database.update_collection(
            cat_path=cat_path, collection_id=collection_id, collection=collection, workspace=workspace
        )

        return CollectionSerializer.db_to_stac(
            collection,
            request,
            extensions=[type(ext).__name__ for ext in self.database.extensions],
        )
    
    @overrides
    async def update_collection_access_policy(
        self, cat_path: str, collection_id: str, access_policy: AccessPolicy, workspace: str, **kwargs
    ) -> None:
        """
        Update a collection access policy.

        This method updates an existing collection in the database by first finding
        the collection by the id given in the keyword argument `collection_id`.
        If no `collection_id` is given the id of the given collection object is used.
        If the object and keyword collection ids don't match the sub items
        collection id is updated else the items are left unchanged.
        The updated collection is then returned.

        Args:
            cat_path: The path of the parent catalog containing the collection.
            collection_id: id of the existing collection to be updated
            collection: A STAC collection that needs to be updated.
            workspace: The workspace making the request.
            kwargs: Additional keyword arguments.

        Returns:
            A STAC collection that has been updated in the database.

        """
        request = kwargs["request"]

        await self.database.update_collection_access_policy(
            cat_path=cat_path, collection_id=collection_id, access_policy=access_policy, workspace=workspace
        )

        return None

    @overrides
    async def delete_collection(
        self, cat_path: str, collection_id: str, workspace: str, **kwargs
    ) -> Optional[stac_types.Collection]:
        """
        Delete a collection.

        This method deletes an existing collection in the database.

        Args:
            cat_path (str): The path of the parent catalog containing the collection.
            collection_id (str): The identifier of the collection that contains the item.
            workspace (str): The workspace making the request.
            kwargs: Additional keyword arguments.

        Returns:
            None.

        Raises:
            NotFoundError: If the collection doesn't exist.
        """
        await self.database.delete_collection(cat_path=cat_path, collection_id=collection_id, workspace=workspace)
        return None
    
    @overrides
    async def create_catalog(
        self, cat_path: str, catalog: Catalog, workspace: str, **kwargs
    ) -> stac_types.Catalog:
        """Create a new catalog in the database.

        Args:
            cat_path (str): The path of the parent catalog containing the catalogs
            catalog (stac_types.Catalog): The catalog to be created.
            workspace (str): The workspace making the request.
            kwargs: Additional keyword arguments.

        Returns:
            stac_types.Catalog: The created catalog object.

        Raises:
            ConflictError: If the catalog already exists.
        """
        catalog = catalog.model_dump(mode="json")
        request = kwargs["request"]
        catalog = self.database.catalog_serializer.stac_to_db(catalog, request)
        await self.database.create_catalog(cat_path=cat_path, catalog=catalog, workspace=workspace)
        return CollectionSerializer.db_to_stac(
            catalog,
            request,
            extensions=[type(ext).__name__ for ext in self.database.extensions],
        )

    @overrides
    async def update_catalog(
        self, cat_path: str, catalog: Catalog, workspace: str, **kwargs
    ) -> stac_types.Catalog:
        """
        Update a catalog.

        This method updates an existing catalog in the database by first finding
        the catalog by the id given in the keyword argument `catalog_id`.
        If no `catalog_id` is given the id of the given catalog object is used.
        The updated catalog is then returned.

        Args:
            cat_path: The path of the catalog to update.
            catalog: A STAC catalog that needs to be updated.
            workspace: The workspace making the request.
            kwargs: Additional keyword arguments.

        Returns:
            A STAC catalog that has been updated in the database.

        """
        catalog = catalog.model_dump(mode="json")

        request = kwargs["request"]

        catalog = self.database.catalog_serializer.stac_to_db(catalog, request)
        await self.database.update_catalog(
            cat_path=cat_path, catalog=catalog, workspace=workspace
        )

        return CatalogSerializer.db_to_stac(
            catalog,
            request,
            extensions=[type(ext).__name__ for ext in self.database.extensions],
        )
    
    @overrides
    async def update_catalog_access_policy(
        self, cat_path: str, access_policy: AccessPolicy, workspace: str, **kwargs
    ) -> None:
        """
        Update a catalog access policy.

        This method updates an existing catalog in the database by first finding
        the catalog by the id given in the keyword argument `collection_id`.
        If no `collection_id` is given the id of the given collection object is used.
        If the object and keyword collection ids don't match the sub items
        collection id is updated else the items are left unchanged.
        The updated collection is then returned.

        Args:
            cat_path: The path of the parent catalog containing the collection.
            access_policy: The access policy to be updated.
            workspace: The workspace making the request.
            kwargs: Additional keyword arguments.

        Returns:
            A STAC catalog that has been updated in the database.

        """
        request = kwargs["request"]

        await self.database.update_catalog_access_policy(
            cat_path=cat_path, access_policy=access_policy, workspace=workspace
        )

        catalog = await self.database.find_catalog(cat_path=cat_path, workspaces=[workspace], user_is_authenticated=True)

        return None

    @overrides
    async def delete_catalog(
        self, cat_path: str, workspace: str, **kwargs
    ) -> Optional[stac_types.Catalog]:
        """
        Delete a catalog.

        This method deletes an existing catalog in the database.

        Args:
            cat_path (str): The path of the parent catalog containing the catalog.
            workspace (str): The workspace making the request.
            kwargs: Additional keyword arguments.

        Returns:
            None.

        Raises:
            NotFoundError: If the catalog doesn't exist.
        """
        await self.database.delete_catalog(cat_path=cat_path, workspace=workspace)
        return None


@attr.s
class BulkTransactionsClient(BaseBulkTransactionsClient):
    """A client for posting bulk transactions to a Postgres database.

    Attributes:
        session: An instance of `Session` to use for database connection.
        database: An instance of `DatabaseLogic` to perform database operations.
    """

    database: BaseDatabaseLogic = attr.ib()
    settings: ApiBaseSettings = attr.ib()
    session: Session = attr.ib(default=attr.Factory(Session.create_from_env))

    def __attrs_post_init__(self):
        """Create es engine."""
        self.client = self.settings.create_client

    def preprocess_item(
        self, item: stac_types.Item, base_url, method: BulkTransactionMethod
    ) -> stac_types.Item:
        """Preprocess an item to match the data model.

        Args:
            item: The item to preprocess.
            base_url: The base URL of the request.
            method: The bulk transaction method.

        Returns:
            The preprocessed item.
        """
        exist_ok = method == BulkTransactionMethod.UPSERT
        return self.database.sync_prep_create_item(
            item=item, base_url=base_url, exist_ok=exist_ok
        )

    @overrides
    def bulk_item_insert(
        self, items: Items, chunk_size: Optional[int] = None, **kwargs
    ) -> str:
        """Perform a bulk insertion of items into the database using Elasticsearch.

        Args:
            items: The items to insert.
            chunk_size: The size of each chunk for bulk processing.
            **kwargs: Additional keyword arguments, such as `request` and `refresh`.

        Returns:
            A string indicating the number of items successfully added.
        """
        request = kwargs.get("request")
        if request:
            base_url = str(request.base_url)
        else:
            base_url = ""

        processed_items = [
            self.preprocess_item(item, base_url, items.method)
            for item in items.items.values()
        ]

        # not a great way to get the collection_id-- should be part of the method signature
        collection_id = processed_items[0]["collection"]

        self.database.bulk_sync(
            collection_id, processed_items, refresh=kwargs.get("refresh", False)
        )

        return f"Successfully added {len(processed_items)} Items."


@attr.s
class EsAsyncBaseFiltersClient(AsyncBaseFiltersClient):
    """Defines a pattern for implementing the STAC filter extension."""

    database: BaseDatabaseLogic = attr.ib()

    # todo: use the ES _mapping endpoint to dynamically find what fields exist
    async def get_queryables(
        self, auth_headers: dict, cat_path: Optional[str] = None, collection_id: Optional[str] = None, **kwargs
    ) -> Dict[str, Any]:
        """Get the queryables available for the given collection_id.

        If collection_id is None, returns the intersection of all
        queryables over all collections.

        This base implementation returns a blank queryable schema. This is not allowed
        under OGC CQL but it is allowed by the STAC API Filter Extension

        https://github.com/radiantearth/stac-api-spec/tree/master/fragments/filter#queryables

        Args:
            auth_headers (dict): The authentication headers.
            cat_path (str, optional): The path of the parent catalog containing the collection.
            collection_id (str, optional): The id of the collection to get queryables for.
            **kwargs: additional keyword arguments

        Returns:
            Dict[str, Any]: A dictionary containing the queryables for the given collection.
        """

        workspaces = auth_headers.get("X-Workspaces", [])
        user_is_authenticated = auth_headers.get("X-Authenticated", False)

        if cat_path and collection_id:
            await self.database.find_collection(cat_path=cat_path, collection_id=collection_id, workspaces=workspaces, user_is_authenticated=user_is_authenticated)
        elif cat_path:
            await self.database.find_catalog(cat_path=cat_path, workspaces=workspaces, user_is_authenticated=user_is_authenticated)

        return {
            "$schema": "https://json-schema.org/draft/2019-09/schema",
            "$id": "https://stac-api.example.com/queryables",
            "type": "object",
            "title": "Queryables for Example STAC API",
            "description": "Queryable names for the example STAC API Item Search filter.",
            "properties": {
                "id": {
                    "description": "ID",
                    "$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/item.json#/definitions/core/allOf/2/properties/id",
                },
                "collection": {
                    "description": "Collection",
                    "$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/item.json#/definitions/core/allOf/2/then/properties/collection",
                },
                "geometry": {
                    "description": "Geometry",
                    "$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/item.json#/definitions/core/allOf/1/oneOf/0/properties/geometry",
                },
                "datetime": {
                    "description": "Acquisition Timestamp",
                    "$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/datetime.json#/properties/datetime",
                },
                "created": {
                    "description": "Creation Timestamp",
                    "$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/datetime.json#/properties/created",
                },
                "updated": {
                    "description": "Creation Timestamp",
                    "$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/datetime.json#/properties/updated",
                },
                "cloud_cover": {
                    "description": "Cloud Cover",
                    "$ref": "https://stac-extensions.github.io/eo/v1.0.0/schema.json#/definitions/fields/properties/eo:cloud_cover",
                },
                "cloud_shadow_percentage": {
                    "description": "Cloud Shadow Percentage",
                    "title": "Cloud Shadow Percentage",
                    "type": "number",
                    "minimum": 0,
                    "maximum": 100,
                },
                "nodata_pixel_percentage": {
                    "description": "No Data Pixel Percentage",
                    "title": "No Data Pixel Percentage",
                    "type": "number",
                    "minimum": 0,
                    "maximum": 100,
                },
            },
            "additionalProperties": True,
        }

@attr.s
class EsAsyncCollectionSearchClient(AsyncBaseCollectionSearchClient):
    """Collection Search extension for searching collections."""
    
    database: BaseDatabaseLogic = attr.ib()
    settings: ApiBaseSettings = attr.ib()
    session: Session = attr.ib(default=attr.Factory(Session.create_from_env))

    collection_serializer: Type[CollectionSerializer] = attr.ib(
        default=CollectionSerializer
    )

    @overrides
    async def post_all_collections(
        self,
        search_request: BaseCollectionSearchPostRequest,
        auth_headers: dict = {},
        cat_path: Optional[str] = None,
        **kwargs,
    ) -> stac_types.Collections:
        """Search all available collections.

        Called with `POST /collections`.

        Returns:
            A list of collections.

        """
        logger.info("Getting all collections (POST)")
        request = kwargs["request"]
        base_url = str(request.base_url)
        workspaces = auth_headers.get("X-Workspaces", [])
        user_is_authenticated = auth_headers.get("X-Authenticated", False)
        limit = int(request.query_params.get("limit", 10))
        token = request.query_params.get("token")

        if cat_path:
            await self.database.find_catalog(cat_path=cat_path, workspaces=workspaces, user_is_authenticated=user_is_authenticated)

        search = self.database.make_search()

        # Apply user access filter
        search = self.database.apply_access_filter(
            search=search, workspaces=workspaces
        )

        if cat_path:
            search = self.database.apply_recursive_catalogs_filter(
                    search=search, catalog_path=cat_path
                )

        if search_request.datetime:
            datetime_search = CoreClient._return_date(search_request.datetime)
            search = self.database.apply_datetime_collections_filter(
                search=search, datetime_search=datetime_search
            )

        if search_request.bbox:
            bbox = search_request.bbox
            if len(bbox) == 6:
                bbox = [bbox[0], bbox[1], bbox[3], bbox[4]]

            search = self.database.apply_bbox_collections_filter(
                search=search, bbox=bbox
            )

        if search_request.q:
            q = search_request.q
            search = self.database.apply_keyword_collections_filter(search=search, q=q)

        if search_request.limit:
            limit = search_request.limit

        collections, maybe_count, next_token = await self.database.execute_collection_search(
            search=search,
            limit=limit,
            token=token,  # type: ignore
            sort=None,
        )

        collections = [
            self.collection_serializer.db_to_stac(collection=collection, request=request) for collection in collections
        ]

        if not cat_path:
            parent_href_url = ""
            collections_href_url = "collections"
        else:
            parent_href_url = f"catalogs/{cat_path}"
            collections_href_url = f"catalogs/{cat_path}/collections"

        links = [
            {"rel": Relations.root.value, "type": MimeTypes.json, "href": base_url},
            {"rel": Relations.parent.value, "type": MimeTypes.json, "href": urljoin(base_url, parent_href_url)},
            {
                "rel": Relations.self.value,
                "type": MimeTypes.json,
                "href": urljoin(base_url, collections_href_url),
            },
        ]

        if next_token:
            next_link = PagingLinks(next=next_token, request=request).link_next()
            links.append(next_link)

        return stac_types.Collections(
            collections=collections,
            links=links,
            numReturned=len(collections),
            numMatched=maybe_count,
        )
