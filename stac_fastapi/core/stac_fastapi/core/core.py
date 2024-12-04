"""Item crud client."""

import logging
import os
import re
from datetime import datetime as datetime_type
from datetime import timezone
from typing import Any, Dict, List, Optional, Set, Type, Union
from urllib.parse import unquote_plus, urljoin

import attr
import orjson
import stac_pydantic
from fastapi import HTTPException, Request
from overrides import overrides
from pydantic import ValidationError
from pygeofilter.backends.cql2_json import to_cql2
from pygeofilter.parsers.cql2_text import parse as parse_cql2_text
from stac_pydantic.links import Relations
from stac_pydantic.shared import MimeTypes
from stac_pydantic.version import STAC_VERSION

from stac_fastapi.core.access_control import create_bitstring, hash_to_index
from stac_fastapi.core.base_database_logic import BaseDatabaseLogic
from stac_fastapi.core.base_settings import ApiBaseSettings
from stac_fastapi.core.models.links import PagingLinks
from stac_fastapi.core.serializers import (
    CatalogCollectionSerializer,
    CatalogSerializer,
    CollectionSerializer,
    ItemSerializer,
)
from stac_fastapi.core.session import Session
from stac_fastapi.core.types.core import (
    AsyncBaseCoreClient,
    AsyncBaseFiltersClient,
    AsyncBaseTransactionsClient,
    AsyncCollectionSearchClient,
    AsyncDiscoverySearchClient,
)
from stac_fastapi.extensions.third_party.bulk_transactions import (
    BaseBulkTransactionsClient,
    BulkTransactionMethod,
    Items,
)
from stac_fastapi.types import stac as stac_types
from stac_fastapi.types.config import Settings
from stac_fastapi.types.conformance import BASE_CONFORMANCE_CLASSES
from stac_fastapi.types.errors import InvalidQueryParameter
from stac_fastapi.types.extension import ApiExtension
from stac_fastapi.types.requests import get_base_url
from stac_fastapi.types.search import (
    BaseCatalogSearchPostRequest,
    BaseCollectionSearchPostRequest,
    BaseDiscoverySearchPostRequest,
    BaseSearchPostRequest,
    Limit,
)
from stac_fastapi.types.stac import (
    Catalog,
    Catalogs,
    CatalogsAndCollections,
    Collection,
    Collections,
    Item,
    ItemCollection,
)

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

NUMBER_OF_CATALOG_COLLECTIONS = os.getenv("NUMBER_OF_CATALOG_COLLECTIONS", Limit.le)


@attr.s
class CoreClient(AsyncBaseCoreClient):
    """Client for core endpoints defined by the STAC specification.

    This class is a implementation of `AsyncBaseCoreClient` that implements the core endpoints
    defined by the STAC specification. It uses the `DatabaseLogic` class to interact with the
    database, and `ItemSerializer` and `CollectionSerializer` to convert between STAC objects and
    database records.

    Attributes:
        session (Session): A requests session instance to be used for all HTTP requests.
        item_serializer (Type[serializers.ItemSerializer]): A serializer class to be used to convert
            between STAC items and database records.
        collection_serializer (Type[serializers.CollectionSerializer]): A serializer class to be
            used to convert between STAC collections and database records.
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
    catalog_post_request_model = attr.ib(default=BaseCatalogSearchPostRequest)
    stac_version: str = attr.ib(default=STAC_VERSION)
    landing_page_id: str = attr.ib(default="stac-fastapi")
    title: str = attr.ib(default="stac-fastapi")
    description: str = attr.ib(default="stac-fastapi")


    def filter_by_access(self, username: str, data: list):
        output_data = []
        for d in data:
            try:
                if d["_sfapi_internal"]["inf_public"] or d["_sfapi_internal"]["owner"] == username:
                    output_data.append(d)
            except KeyError:
                logger.error(f"No access control found for catalog {d['id']}")
        return output_data



    def _landing_page(
        self,
        base_url: str,
        conformance_classes: List[str],
        extension_schemas: List[str],
    ) -> stac_types.LandingPage:
        logger.debug("Creating landing page")
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

    async def root_landing_page(
        self, username_header: dict, catalog_path: Optional[str] = None, **kwargs
    ) -> stac_types.LandingPage:
        """Landing page.

        Called with `GET /`.

        Args:
            username_header (dict): X-Username header from the request.
            catalog_path (str):
            **kwargs: Keyword arguments from the request.

        Returns:
            API landing page, serving as an entry point to the API.
        """
        logger.info("Getting landing page")
        request: Request = kwargs["request"]
        base_url = get_base_url(request)
        landing_page = self._landing_page(
            base_url=base_url,
            conformance_classes=self.conformance_classes(),
            extension_schemas=[],
        )

        # Check if current user has access to each Catalog
        # Extract X-Username header from username_header
        username = username_header.get("X-Username", "")

        # Get user index
        user_index = hash_to_index(username)

        catalogs = []

        temp_catalogs = await self.database.get_catalog_subcatalogs(base_url=base_url)

        catalogs = self.filter_by_access(username, temp_catalogs)

        for catalog in catalogs:
            landing_page["links"].append(
                {
                    "rel": Relations.child.value,
                    "type": MimeTypes.json.value,
                    "title": catalog.get("title") or catalog.get("id"),
                    "href": urljoin(base_url, f"catalogs/{catalog['id']}"),
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

    async def landing_page(
        self, username_header: dict, catalog_path: Optional[str] = None, **kwargs
    ) -> stac_types.LandingPage:
        """Landing page.

        Called with `GET /`.

        Args:
            username_header (dict): X-Username header from the request.
            catalog_path (str): The path to the catalog for this landing page.
            **kwargs: Keyword arguments from the request.

        Returns:
            API landing page, serving as an entry point to the API.
        """
        logger.info("Getting landing page")
        request: Request = kwargs["request"]
        base_url = get_base_url(request)
        landing_page = self._landing_page(
            base_url=f"{base_url}catalogs/{catalog_path}/",
            conformance_classes=self.conformance_classes(),
            extension_schemas=[],
        )

        catalog = await self.get_catalog(username_header, catalog_path, request=request)
        landing_page.update(
            {
                "id": catalog["id"],
                "title": catalog["title"],
                "description": catalog["description"],
                "stac_version": catalog["stac_version"],
            }
        )
        for link in landing_page["links"]:
            # Replace conformance link to be root link
            if link["rel"] == Relations.conformance.value:
                link["href"] = urljoin(str(request.base_url), "conformance")
                break

        if "links" in catalog:
            for link in catalog["links"]:
                if link["rel"] == "child":
                    landing_page["links"].append(link)

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

    async def all_collections(self, username_header: dict, **kwargs) -> Collections:
        """Read all collections from the database.

        Args:
            username_header (dict): X-Username header from the request.
            **kwargs: Keyword arguments from the request.

        Returns:
            A Collections object containing all the collections in the database and links to various resources.
        """
        logger.info("Getting all collections")
        request = kwargs["request"]
        base_url = str(request.base_url)
        limit = int(request.query_params.get("limit", 10))
        token = request.query_params.get("token")

        # Check if current user has access to each Catalog
        # Extract X-Username header from username_header
        username = username_header.get("X-Username", "")

        # Get user index
        user_index = hash_to_index(username)

        collections = []

        collections, next_token = (
            await self.database.get_all_collections(
                token=token, limit=limit, base_url=base_url, user_index=user_index
            )
        )

        links = [
            {"rel": Relations.root.value, "type": MimeTypes.json, "href": base_url},
            {"rel": Relations.parent.value, "type": MimeTypes.json, "href": base_url},
            {
                "rel": Relations.self.value,
                "type": MimeTypes.json,
                "href": urljoin(base_url, "collections"),
            },
        ]

        if next_token:
            next_link = PagingLinks(next=next_token, request=request).link_next()
            links.append(next_link)

        return Collections(collections=collections, links=links)

    async def all_catalogs(
        self, username_header: dict, catalog_path: Optional[str] = None, **kwargs
    ) -> Catalogs:
        """Read all catalogs from the database.

        Args:
            username_header (dict): X-Username header from the request.
            **kwargs: Keyword arguments from the request.

        Returns:
            A Catalogs object containing all the catalogs in the database and links to various resources.
        """
        logger.info("Getting all catalogs")
        request = kwargs["request"]
        base_url = str(request.base_url)
        limit = int(request.query_params.get("limit", 10))
        token = request.query_params.get("token")

        # Extract X-Username header from username_header
        username = username_header.get("X-Username", "")

        # Get user index
        user_index = hash_to_index(username)

        if catalog_path:
            # Check if current user has access to each Catalog
            catalog = await self.database.find_catalog(catalog_path=catalog_path)

            # Get access control array for each catalog
            try:
                # Check access control
                if not catalog["_sfapi_internal"]["owner"] == username:
                    if not catalog["_sfapi_internal"]["inf_public"]:  # Catalog is private
                        if username == "":  # User is not logged in
                            raise HTTPException(
                                status_code=401, detail="User is not authenticated"
                            )
                        else:  # User is logged in but not authorized
                            raise HTTPException(
                                status_code=403,
                                detail="User does not have access to this Catalog",
                            )
            except KeyError:
                logger.error(f"No access control found for catalog {catalog['id']}")
                return Catalogs(catalogs=[], links=[])

        # Search is run continually until limit is reached or no more results
        catalogs, next_token = (
            await self.database.get_all_catalogs(
                catalog_path=catalog_path,
                token=token,
                limit=limit,
                base_url=base_url,
                user_index=user_index,
                conformance_classes=self.conformance_classes(),
            )
        )

        links = [
            {"rel": Relations.root.value, "type": MimeTypes.json, "href": base_url},
            {"rel": Relations.parent.value, "type": MimeTypes.json, "href": base_url},
            {
                "rel": Relations.self.value,
                "type": MimeTypes.json,
                "href": urljoin(base_url, "catalogs"),
            },
        ]

        if next_token:
            next_link = PagingLinks(next=next_token, request=request).link_next()
            links.append(next_link)

        return Catalogs(catalogs=catalogs, links=links)

    async def get_collection(
        self, username_header: dict, catalog_path: str, collection_id: str, **kwargs
    ) -> Collection:
        """Get a collection from the database by its id.

        Args:
            username_header (dict): X-Username header from the request.
            catalog_path (str): The path to the catalog the collection belongs to.
            collection_id (str): The id of the collection to retrieve.
            kwargs: Additional keyword arguments passed to the API call.

        Returns:
            Collection: A `Collection` object representing the requested collection.

        Raises:
            NotFoundError: If the collection with the given id cannot be found in the database.
        """
        logger.info("Getting collection")
        base_url = str(kwargs["request"].base_url)

        collection = await self.database.find_collection(
            catalog_path=catalog_path, collection_id=collection_id
        )

        # Check if current user has access to this Collection
        # Extract X-Username header from username_header
        username = username_header.get("X-Username", "")

        # Get user index
        user_index = hash_to_index(username)
        # Get access control array for each collection
        try:
            # Check access control
            if not collection["_sfapi_internal"]["owner"] == username:
                if not collection["_sfapi_internal"]["inf_public"]:  # Collection is private
                    if username == "":  # User is not logged in
                        raise HTTPException(
                            status_code=401, detail="User is not authenticated"
                        )
                    else:  # User is logged in but not authorized
                        raise HTTPException(
                            status_code=403,
                            detail="User does not have access to this Collection",
                        )
        except KeyError:
            logger.error(f"No access control found for collection {collection['id']}")
            if username == "":  # User is not logged in
                raise HTTPException(status_code=401, detail="User is not authenticated")
            else:  # User is logged in but still can't determine access
                raise HTTPException(
                    status_code=403,
                    detail="User does not have access to this Collection",
                )

        return self.collection_serializer.db_to_stac(
            catalog_path=catalog_path, collection=collection, base_url=base_url
        )

    async def get_catalog(
        self, username_header: dict, catalog_path: str, **kwargs
    ) -> Catalog:
        """Get a catalog from the database by its id.

        Args:
            username_header (dict): X-Username header from the request.
            catalog_path (str): The path to the catalog to retrieve.
            kwargs: Additional keyword arguments passed to the API call.

        Returns:
            Catalog: A `Catalog` object representing the requested collection.

        Raises:
            NotFoundError: If the catalog with the given id cannot be found in the database.
        """
        logger.info("Getting catalog")
        # Identify parent catalog path, where available
        catalog_path_list = catalog_path.split("/")
        if len(catalog_path_list) > 1:
            parent_catalog_path = "/".join(catalog_path_list[:-1])
        else:
            parent_catalog_path = None

        base_url = str(kwargs["request"].base_url)
        catalog = await self.database.find_catalog(catalog_path=catalog_path)

        # Check if current user has access to this Catalog
        # Extract X-Username header from username_header
        username = username_header.get("X-Username", "")

        # Get user index
        user_index = hash_to_index(username)
        # Get access control array for each catalog
        try:
            # Check access control
            if not catalog["_sfapi_internal"]["owner"] == username:
                if not catalog["_sfapi_internal"]["inf_public"]:  # Catalog is private
                    if username == "":  # User is not logged in
                        raise HTTPException(
                            status_code=401, detail="User is not authenticated"
                        )
                    else:  # User is logged in but not authorized
                        raise HTTPException(
                            status_code=403,
                            detail="User does not have access to this Catalog",
                        )
        except KeyError:
            logger.error(f"No access control found for catalog {catalog['id']}")
            if username == "":  # User is not logged in
                raise HTTPException(status_code=401, detail="User is not authenticated")
            else:  # User is logged in but still can't determine access
                raise HTTPException(
                    status_code=403, detail="User does not have access to this Catalog"
                )

        # Assume at most 100 collections in a catalog for the time being, may need to increase
        temp_collections = await self.database.get_catalog_collections(
            catalog_path=catalog_path,
            base_url=base_url,
            limit=NUMBER_OF_CATALOG_COLLECTIONS,
            token=None,
            user_index=user_index,
        )

        # Check if current user has access to each collection
        collections = self.filter_by_access(username, temp_collections)

        temp_sub_catalogs = await self.database.get_catalog_subcatalogs(
            catalog_path=catalog_path,
            base_url=base_url,
        )

        # Check if current user has access to each collection
        sub_catalogs = self.filter_by_access(username, temp_sub_catalogs)

        return self.catalog_serializer.db_to_stac(
            catalog_path=parent_catalog_path,
            catalog=catalog,
            base_url=base_url,
            collections=collections,
            sub_catalogs=sub_catalogs,
            conformance_classes=self.conformance_classes(),
        )

    async def item_collection(
        self,
        username_header: dict,
        catalog_path: str,
        collection_id: str,
        bbox: Optional[List[NumType]] = None,
        datetime: Union[str, datetime_type, None] = None,
        limit: int = 10,
        token: str = None,
        **kwargs,
    ) -> ItemCollection:
        """Read items from a specific collection in the database.

        Args:
            username_header (dict): X-Username header from the request.
            catalog_path (str): The path to the catalog to read items from.
            collection_id (str): The identifier of the collection to read items from.
            bbox (Optional[List[NumType]]): The bounding box to filter items by.
            datetime (Union[str, datetime_type, None]): The datetime range to filter items by.
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
        logger.info("Getting item collection")
        request: Request = kwargs["request"]

        # Get Collection to confirm user access
        collection = await self.database.find_collection(
            catalog_path=catalog_path, collection_id=collection_id
        )

        # Check if current user has access to this Collection
        # Extract X-Username header from username_header
        username = username_header.get("X-Username", "")

        # Get user index
        user_index = hash_to_index(username)

        # Get access control array for the collection
        try:
            # Check access control
            if not collection["_sfapi_internal"]["owner"] == username:
                if not collection["_sfapi_internal"]["inf_public"]:  # Collection is private
                    if username == "":  # User is not logged in
                        raise HTTPException(
                            status_code=401, detail="User is not authenticated"
                        )
                    else:  # User is logged in but not authorized
                        raise HTTPException(
                            status_code=403,
                            detail="User does not have access to this Collection",
                        )
        except KeyError:
            logger.error(f"No access control found for collection {collection['id']}")
            if username == "":  # User is not logged in
                raise HTTPException(status_code=401, detail="User is not authenticated")
            else:  # User is logged in but still can't determine access
                raise HTTPException(
                    status_code=403,
                    detail="User does not have access to this Collection",
                )

        base_url = str(request.base_url)

        search = self.database.make_search()
        search = self.database.apply_collections_filter(
            search=search, collection_ids=[collection_id]
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

        # No further access control needed as already checked above for collection
        items_and_cat_paths, maybe_count, next_token = await self.database.execute_search(
            search=search,
            catalog_paths=[catalog_path],
            limit=limit,
            sort=None,
            token=token,  # type: ignore
            user_index=user_index,
            collection_ids=[collection_id],
        )

        # To handle catalog_id in links execute_search also returns the catalog_id
        # from search results in a tuple
        items = [
            self.item_serializer.db_to_stac(
                catalog_path=catalog_path, item=item_and_cat_path[0], base_url=base_url
            )
            for item_and_cat_path in items_and_cat_paths
        ]

        context_obj = None
        if self.extension_is_enabled("ContextExtension"):
            context_obj = {
                "returned": len(items),
                "limit": limit,
            }
            if maybe_count is not None:
                context_obj["matched"] = maybe_count

        links = []
        if next_token:
            links = await PagingLinks(request=request, next=next_token).get_links()

        return ItemCollection(
            type="FeatureCollection",
            features=items,
            links=links,
            context=context_obj,
        )

    async def get_item(
        self,
        username_header: dict,
        item_id: str,
        collection_id: str,
        catalog_path: str,
        **kwargs,
    ) -> Item:
        """Get an item from the database based on its id and collection id.

        Args:
            username_header (dict): X-Username header from the request.
            item_id (str): The ID of the item to be retrieved.
            collection_id (str): The ID of the collection the item belongs to.
            catalog_path (str): The path to the catalog the collection and item belongs to.
            kwargs: Additional keyword arguments passed to the API call.

        Returns:
            Item: An `Item` object representing the requested item.

        Raises:
            Exception: If any error occurs while getting the item from the database.
            NotFoundError: If the item does not exist in the specified collection.
        """
        logger.info("Getting item")
        base_url = str(kwargs["request"].base_url)

        # Load parent collection to check user access
        collection = await self.database.find_collection(
            catalog_path=catalog_path, collection_id=collection_id
        )

        # Check if current user has access to this item
        # Extract X-Username header from username_header
        username = username_header.get("X-Username", "")

        # Get user index
        user_index = hash_to_index(username)
        # Get access control array for each collection
        try:
            # Check access control
            if not collection["_sfapi_internal"]["owner"] == username:
                if not collection["_sfapi_internal"]["inf_public"]:  # Collection is private
                    if username == "":  # User is not logged in
                        raise HTTPException(
                            status_code=401, detail="User is not authenticated"
                        )
                    else:  # User is logged in but not authorized
                        raise HTTPException(
                            status_code=403,
                            detail="User does not have access to this Collection",
                        )
        except KeyError:
            logger.error(f"No access control found for collection {collection['id']}")
            if username == "":  # User is not logged in
                raise HTTPException(status_code=401, detail="User is not authenticated")
            else:  # User is logged in but still can't determine access
                raise HTTPException(
                    status_code=403,
                    detail="User does not have access to this Collection",
                )

        item = await self.database.get_one_item(
            item_id=item_id,
            collection_id=collection_id,
            catalog_path=catalog_path,
        )

        return self.item_serializer.db_to_stac(
            catalog_path=catalog_path, item=item, base_url=base_url
        )

    @staticmethod
    def _return_date(interval_str):
        """
        Convert a date interval string into a dictionary for filtering search results.

        The date interval string should be formatted as either a single date or a range of dates separated
        by "/". The date format should be ISO-8601 (YYYY-MM-DDTHH:MM:SSZ). If the interval string is a
        single date, it will be converted to a dictionary with a single "eq" key whose value is the date in
        the ISO-8601 format. If the interval string is a range of dates, it will be converted to a
        dictionary with "gte" (greater than or equal to) and "lte" (less than or equal to) keys. If the
        interval string is a range of dates with ".." instead of "/", the start and end dates will be
        assigned default values to encompass the entire possible date range.

        Args:
            interval_str (str): The date interval string to be converted.

        Returns:
            dict: A dictionary representing the date interval for use in filtering search results.
        """
        intervals = interval_str
        if type(intervals) != tuple:
            return {"eq": intervals}
        else:
            start_date = intervals[0]
            end_date = intervals[1]
            if None not in intervals:
                pass
            elif start_date:
                end_date = "2200-12-01T12:31:12Z"
            elif end_date:
                start_date = "1900-10-01T00:00:00Z"
            else:
                start_date = "1900-10-01T00:00:00Z"
                end_date = "2200-12-01T12:31:12Z"

        return {"lte": end_date, "gte": start_date}

    async def get_global_search(
        self,
        request: Request,
        username_header: dict,
        collections: Optional[List[str]] = None,
        catalog_paths: Optional[List[str]] = None,
        ids: Optional[List[str]] = None,
        bbox: Optional[List[NumType]] = None,
        datetime: Optional[Union[str, datetime_type]] = None,
        limit: Optional[int] = 10,
        query: Optional[str] = None,
        token: Optional[str] = None,
        fields: Optional[List[str]] = None,
        sortby: Optional[str] = None,
        intersects: Optional[str] = None,
        filter: Optional[str] = None,
        filter_lang: Optional[str] = None,
        **kwargs,
    ) -> ItemCollection:
        """Get search results from the database.

        Args:
            request (Request): The incoming request.
            username_header (dict): X-Username header from the request.
            collections (Optional[List[str]]): List of collection IDs to search in.
            catalog_paths (Optional[List[str]]): List of catalog paths to search in.
            ids (Optional[List[str]]): List of item IDs to search for.
            bbox (Optional[List[NumType]]): Bounding box to search in.
            datetime (Optional[Union[str, datetime_type]]): Filter items based on the datetime field.
            limit (Optional[int]): Maximum number of results to return.
            query (Optional[str]): Query string to filter the results.
            token (Optional[str]): Access token to use when searching the catalog.
            fields (Optional[List[str]]): Fields to include or exclude from the results.
            sortby (Optional[str]): Sorting options for the results.
            intersects (Optional[str]): GeoJSON geometry to search in.
            filter (Optional[str]): Filter to apply to the search results.
            filter_lang (Optional[str]): Language of the filter to apply.
            **kwargs: Additional parameters to be passed to the API.

        Returns:
            ItemCollection: Collection of `Item` objects representing the search results.

        Raises:
            HTTPException: If any error occurs while searching the catalog.
        """
        logger.info("Performing global GET search")
        base_args = {
            "collections": collections,
            "catalog_paths": catalog_paths,
            "ids": ids,
            "bbox": bbox,
            "limit": limit,
            "token": token,
            "query": orjson.loads(query) if query else query,
        }

        # this is borrowed from stac-fastapi-pgstac
        # Kludgy fix because using factory does not allow alias for filter-lan
        query_params = str(request.query_params)
        if filter_lang is None:
            match = re.search(r"filter-lang=([a-z0-9-]+)", query_params, re.IGNORECASE)
            if match:
                filter_lang = match.group(1)

        if datetime:
            base_args["datetime"] = datetime

        if intersects:
            base_args["intersects"] = orjson.loads(unquote_plus(intersects))

        if sortby:
            sort_param = []
            for sort in sortby:
                sort_param.append(
                    {
                        "field": sort[1:],
                        "direction": "desc" if sort[0] == "-" else "asc",
                    }
                )
            base_args["sortby"] = sort_param

        if filter:
            if filter_lang == "cql2-json":
                base_args["filter-lang"] = "cql2-json"
                base_args["filter"] = orjson.loads(unquote_plus(filter))
            else:
                base_args["filter-lang"] = "cql2-json"
                base_args["filter"] = orjson.loads(to_cql2(parse_cql2_text(filter)))

        if fields:
            includes = set()
            excludes = set()
            for field in fields:
                if field[0] == "-":
                    excludes.add(field[1:])
                elif field[0] == "+":
                    includes.add(field[1:])
                else:
                    includes.add(field)
            base_args["fields"] = {"include": includes, "exclude": excludes}

        # Do the request
        try:
            search_request = self.post_request_model(**base_args)
        except ValidationError:
            raise HTTPException(status_code=400, detail="Invalid parameters provided")
        resp = await self.post_global_search(
            search_request=search_request,
            request=request,
            username_header=username_header,
        )

        return resp

    async def post_global_search(
        self,
        search_request: BaseSearchPostRequest,
        request: Request,
        username_header: dict,
    ) -> ItemCollection:
        """
        Perform a POST search on the catalog.

        Args:
            search_request (BaseSearchPostRequest): Request object that includes the parameters for the search.
            request (Request): The incoming request.
            username_header (dict): X-Username header from the request.

        Returns:
            ItemCollection: A collection of items matching the search criteria.

        Raises:
            HTTPException: If there is an error with the cql2_json filter.
        """
        logger.info("Performing global POST search")
        base_url = str(request.base_url)

        search = self.database.make_search()

        # Can only provide collections if you also provide the containing catalogs
        if search_request.collections and not search_request.catalog_paths:
            raise InvalidQueryParameter(
                "To search specific collection(s), you must provide the containing catalog."
            )
        # Can only provide collections if you also provide the single containing catalog
        elif search_request.collections and len(search_request.catalog_paths) > 1:
            raise InvalidQueryParameter(
                "To search specific collections, you must provide only one containing catalog."
            )

        specified_catalog_paths = True
        specified_collections = True

        if not search_request.catalog_paths:
            search_request.catalog_paths = []
            specified_catalog_paths = False

        if not search_request.collections:
            search_request.collections = []
            specified_collections = False

        if search_request.ids:
            search = self.database.apply_ids_filter(
                search=search, item_ids=search_request.ids
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
                    search = self.database.apply_stacql_filter(
                        search=search, op=op, field=field, value=value
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

        sort = None
        if search_request.sortby:
            sort = self.database.populate_sort(search_request.sortby)

        limit = 10
        if search_request.limit:
            limit = search_request.limit

        token = None
        if search_request.token:
            token = search_request.token

        # Extract X-Username header from username_header
        username = username_header.get("X-Username", "")

        # Get user index
        user_index = hash_to_index(username)

        # Filter the search catalogs to those that are accessible to the user
        for catalog_path in search_request.catalog_paths[:]:
            catalog = await self.database.find_catalog(catalog_path=catalog_path)
            # Get access control array for each catalog
            try:
                # Remove catalog from list if user does not have access
                if not catalog["_sfapi_internal"]["owner"] == username and not catalog["_sfapi_internal"]["inf_public"]:
                    search_request.catalog_paths.remove(catalog_path)
            except KeyError:
                logger.error(f"No access control found for catalog {catalog['id']}")
                search_request.catalog_paths.remove(catalog_path)

        if search_request.catalog_paths:
            # Filter the search collections to those that are accessible to the user
            for collection_id in search_request.collections[:]:
                collection = await self.database.find_collection(
                    catalog_path=search_request.catalog_paths[0],
                    collection_id=collection_id,
                )
                # Get access control array for each collection
                try:
                    # Remove catalog from list if user does not have access
                    if not collection["_sfapi_internal"]["owner"] == username and not collection["_sfapi_internal"]["inf_public"]:
                        search_request.collections.remove(collection_id)
                except KeyError:
                    logger.error(
                        f"No access control found for collection {collection['id']}"
                    )
                    search_request.collections.remove(collection_id)

        items = []

        if specified_catalog_paths and not search_request.catalog_paths:
            return ItemCollection(
                type="FeatureCollection",
                features=items,
                links=[],
                context={"returned": 0, "limit": limit},
            )

        if specified_collections and not search_request.collections:
            return ItemCollection(
                type="FeatureCollection",
                features=items,
                links=[],
                context={"returned": 0, "limit": limit},
            )

        items_and_cat_paths, maybe_count, next_token = (
            await self.database.execute_search(
                search=search,
                limit=limit,
                token=token,  # type: ignore
                sort=sort,
                user_index=user_index,
                collection_ids=search_request.collections,
                catalog_paths=search_request.catalog_paths,
            )
        )

        # To handle catalog_id in links execute_search also returns the catalog_id
        # from search results in a tuple
        items = [
            self.item_serializer.db_to_stac(
                item=item_and_cat_path[0], base_url=base_url, catalog_path=item_and_cat_path[1]
            )
            for item_and_cat_path in items_and_cat_paths
        ]

        if self.extension_is_enabled("FieldsExtension"):
            if search_request.query is not None:
                query_include: Set[str] = set(
                    [
                        k if k in Settings.get().indexed_fields else f"properties.{k}"
                        for k in search_request.query.keys()
                    ]
                )
                if not search_request.fields.include:
                    search_request.fields.include = query_include
                else:
                    search_request.fields.include.union(query_include)

            filter_kwargs = search_request.fields.filter_fields

            items = [
                orjson.loads(
                    stac_pydantic.Item(**feat).json(**filter_kwargs, exclude_unset=True)
                )
                for feat in items
            ]

        context_obj = None
        if self.extension_is_enabled("ContextExtension"):
            context_obj = {
                "returned": len(items),
                "limit": limit,
            }
            if maybe_count is not None:
                context_obj["matched"] = maybe_count

        links = []
        if next_token:
            links = await PagingLinks(request=request, next=next_token).get_links()

        return ItemCollection(
            type="FeatureCollection",
            features=items,
            links=links,
            context=context_obj,
        )

    async def get_search(
        self,
        request: Request,
        username_header: dict,
        catalog_path: Optional[str],
        collections: Optional[List[str]] = None,
        ids: Optional[List[str]] = None,
        bbox: Optional[List[NumType]] = None,
        datetime: Optional[Union[str, datetime_type]] = None,
        limit: Optional[int] = 10,
        query: Optional[str] = None,
        token: Optional[str] = None,
        fields: Optional[List[str]] = None,
        sortby: Optional[str] = None,
        intersects: Optional[str] = None,
        filter: Optional[str] = None,
        filter_lang: Optional[str] = None,
        **kwargs,
    ) -> ItemCollection:
        """Get search results from the database in a specific catalog.

        Args:
            request (Request): The incoming request.
            username_header (dict): X-Username header from the request.
            catalog_path (Optional[[str]): Path to catalog to search in.
            collections (Optional[List[str]]): List of collection IDs to search in.
            ids (Optional[List[str]]): List of item IDs to search for.
            bbox (Optional[List[NumType]]): Bounding box to search in.
            datetime (Optional[Union[str, datetime_type]]): Filter items based on the datetime field.
            limit (Optional[int]): Maximum number of results to return.
            query (Optional[str]): Query string to filter the results.
            token (Optional[str]): Access token to use when searching the catalog.
            fields (Optional[List[str]]): Fields to include or exclude from the results.
            sortby (Optional[str]): Sorting options for the results.
            intersects (Optional[str]): GeoJSON geometry to search in.
            kwargs: Additional parameters to be passed to the API.

        Returns:
            ItemCollection: Collection of `Item` objects representing the search results.

        Raises:
            HTTPException: If any error occurs while searching the catalog.
        """
        logger.info("Performing GET search")
        base_args = {
            "collections": collections,
            "ids": ids,
            "bbox": bbox,
            "limit": limit,
            "token": token,
            "query": orjson.loads(query) if query else query,
        }

        # this is borrowed from stac-fastapi-pgstac
        # Kludgy fix because using factory does not allow alias for filter-lan
        query_params = str(request.query_params)
        if filter_lang is None:
            match = re.search(r"filter-lang=([a-z0-9-]+)", query_params, re.IGNORECASE)
            if match:
                filter_lang = match.group(1)

        if datetime:
            base_args["datetime"] = datetime

        if intersects:
            base_args["intersects"] = orjson.loads(unquote_plus(intersects))

        if sortby:
            sort_param = []
            for sort in sortby:
                sort_param.append(
                    {
                        "field": sort[1:],
                        "direction": "desc" if sort[0] == "-" else "asc",
                    }
                )
            base_args["sortby"] = sort_param

        if filter:
            if filter_lang == "cql2-json":
                base_args["filter-lang"] = "cql2-json"
                base_args["filter"] = orjson.loads(unquote_plus(filter))
            else:
                base_args["filter-lang"] = "cql2-json"
                base_args["filter"] = orjson.loads(to_cql2(parse_cql2_text(filter)))

        if fields:
            includes = set()
            excludes = set()
            for field in fields:
                if field[0] == "-":
                    excludes.add(field[1:])
                elif field[0] == "+":
                    includes.add(field[1:])
                else:
                    includes.add(field)
            base_args["fields"] = {"include": includes, "exclude": excludes}

        # Do the request
        try:
            search_request = self.catalog_post_request_model(**base_args)
        except ValidationError:
            raise HTTPException(status_code=400, detail="Invalid parameters provided")
        resp = await self.post_search(
            catalog_path=catalog_path,
            search_request=search_request,
            request=request,
            username_header=username_header,
        )

        return resp

    async def post_search(
        self,
        catalog_path: Optional[str],
        search_request: BaseCatalogSearchPostRequest,
        request: Request,
        username_header: dict,
        **kwargs,
    ) -> ItemCollection:
        """
        Perform a POST search on a specific sub-catalog.

        Args:
            catalog_path (Optional[str]): Path to catalog to search in.
            search_request (BaseCatalogSearchPostRequest): Request object that includes the parameters for the search.
            request (Request): The incoming request.
            username_header (dict): X-Username header from the request.
            **kwargs: Keyword arguments passed to the function.

        Returns:
            ItemCollection: A collection of items matching the search criteria.

        Raises:
            HTTPException: If there is an error with the cql2_json filter.
        """
        logger.info("Performing POST search")
        base_url = str(request.base_url)

        # Check catalog is accessible to the user
        # Extract X-Username header from username_header
        username = username_header.get("X-Username", "")

        # Get user index
        user_index = hash_to_index(username)

        # Filter the search catalogs to those that are accessible to the user
        catalog = await self.database.find_catalog(catalog_path=catalog_path)
        # Get access control array for each catalog
        try:
            # Check access control
            if not catalog["_sfapi_internal"]["owner"] == username:
                if not catalog["_sfapi_internal"]["inf_public"]:  # Collection is private
                    if username == "":  # User is not logged in
                        raise HTTPException(
                            status_code=401, detail="User is not authenticated"
                        )
                    else:  # User is logged in but not authorized
                        raise HTTPException(
                            status_code=403,
                            detail="User does not have access to this Catalog",
                        )
        except KeyError:
            logger.error(f"No access control found for catalog {catalog['id']}")
            if username == "":  # User is not logged in
                raise HTTPException(status_code=401, detail="User is not authenticated")
            else:  # User is logged in but still can't determine access
                raise HTTPException(
                    status_code=403, detail="User does not have access to this Catalog"
                )

        collections = []
        if search_request.collections:
            collections = search_request.collections

        # Filter the search collections to those that are accessible to the user
        for collection_id in collections[:]:
            # Filter the search catalogs to those that are accessible to the user
            collection = await self.database.find_collection(
                catalog_path=catalog_path, collection_id=collection_id
            )
            # Get access control array for each collection
            try:
                # Remove catalog from list if user does not have access
                if not collection["_sfapi_internal"]["owner"] == username and not collection["_sfapi_internal"]["inf_public"]:
                    collections.remove(collection_id)
            except KeyError:
                logger.error(
                    f"No access control found for collection {collection['id']}"
                )
                collections.remove(collection_id)

        search = self.database.make_search()

        if search_request.ids:
            search = self.database.apply_ids_filter(
                search=search, item_ids=search_request.ids
            )

        if collections:
            search = self.database.apply_collections_filter(
                search=search, collection_ids=collections
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
                    search = self.database.apply_stacql_filter(
                        search=search, op=op, field=field, value=value
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

        sort = None
        if search_request.sortby:
            sort = self.database.populate_sort(search_request.sortby)

        limit = 10
        if search_request.limit:
            limit = search_request.limit

        token = None
        if search_request.token:
            token = search_request.token

        items = []

        items_and_cat_paths, maybe_count, next_token = (
            await self.database.execute_search(
                search=search,
                limit=limit,
                token=token,  # type: ignore
                sort=sort,
                collection_ids=collections,
                catalog_paths=[catalog_path],
                user_index=user_index,
            )
        )

        # To handle catalog_id in links execute_search also returns the catalog_id
        # from search results in a tuple
        items = [
            self.item_serializer.db_to_stac(
                item=item_and_cat_path[0], base_url=base_url, catalog_path=item_and_cat_path[1]
            )
            for item_and_cat_path in items_and_cat_paths
        ]

        if self.extension_is_enabled("FieldsExtension"):
            if search_request.query is not None:
                query_include: Set[str] = set(
                    [
                        k if k in Settings.get().indexed_fields else f"properties.{k}"
                        for k in search_request.query.keys()
                    ]
                )
                if not search_request.fields.include:
                    search_request.fields.include = query_include
                else:
                    search_request.fields.include.union(query_include)

            filter_kwargs = search_request.fields.filter_fields

            items = [
                orjson.loads(
                    stac_pydantic.Item(**feat).json(**filter_kwargs, exclude_unset=True)
                )
                for feat in items
            ]

        context_obj = None
        if self.extension_is_enabled("ContextExtension"):
            context_obj = {
                "returned": len(items),
                "limit": limit,
            }
            if maybe_count is not None:
                context_obj["matched"] = maybe_count

        links = []
        if next_token:
            links = await PagingLinks(request=request, next=next_token).get_links()

        return ItemCollection(
            type="FeatureCollection",
            features=items,
            links=links,
            context=context_obj,
        )


@attr.s
class TransactionsClient(AsyncBaseTransactionsClient):
    """Transactions extension specific CRUD operations."""

    database: BaseDatabaseLogic = attr.ib()
    settings: ApiBaseSettings = attr.ib()
    session: Session = attr.ib(default=attr.Factory(Session.create_from_env))

    @overrides
    async def create_item(
        self,
        catalog_path: str,
        collection_id: str,
        item: stac_types.Item,
        workspace: str,
        **kwargs,
    ) -> Optional[stac_types.Item]:
        """Create an item in the collection.
        Note, access for items is determined by parent collection or catalog

        Args:
            catalog_path (str): The path to the catalog containing the parent collection.
            collection_id (str): The id of the collection to add the item to.
            item (stac_types.Item): The item to be added to the collection.
            username_header (dict): X-Username header from the request.
            workspace (str): The workspace being used to create the item.
            **kwargs: Additional keyword arguments.

        Returns:
            stac_types.Item: The created item.

        Raises:
            NotFound: If the specified collection is not found in the database.
            ConflictError: If the item in the specified collection already exists.

        """
        logger.info("Creating item")
        if not item:
            raise HTTPException(status_code=400, detail="No item provided")

        base_url = str(kwargs["request"].base_url)

        # Confirm user has access to parent collection
        # Retrieve catalog to confirm owning workspace
        collection = await self.database.find_collection(
            collection_id=collection_id, catalog_path=catalog_path
        )
        owner = collection["_sfapi_internal"]["owner"]

        if owner != workspace:
            raise HTTPException(
                status_code=403, detail="Workspace does not have access to the parent Collection"
            )

        if collection_id != item["collection"]:
            raise Exception(
                f"The provided collection id and that found in the item do not match: {collection_id} : {item['collection']}"
            )

        # If a feature collection is posted
        if item["type"] == "FeatureCollection":
            bulk_client = BulkTransactionsClient(
                database=self.database, settings=self.settings
            )
            processed_items = [
                bulk_client.preprocess_item(item, base_url, BulkTransactionMethod.INSERT) for item in item["features"]  # type: ignore
            ]

            await self.database.bulk_async(
                catalog_path=catalog_path,
                collection_id=collection_id,
                processed_items=processed_items,
                refresh=kwargs.get("refresh", False),
            )

            return None
        else:
            item = await self.database.prep_create_item(
                catalog_path=catalog_path, item=item, base_url=base_url
            )
            await self.database.create_item(
                catalog_path=catalog_path,
                item=item,
                refresh=kwargs.get("refresh", False),
            )
            return item

    @overrides
    async def update_item(
        self,
        catalog_path: str,
        collection_id: str,
        item_id: str,
        item: stac_types.Item,
        workspace: str,
        **kwargs,
    ) -> stac_types.Item:
        """Update an item in the collection.

        Args:
            collection_id (str): The ID of the collection the item belongs to.
            item_id (str): The ID of the item to be updated.
            item (stac_types.Item): The new item data.
            kwargs: Other optional arguments, including the request object.

        Returns:
            stac_types.Item: The updated item object.

        Raises:
            NotFound: If the specified collection is not found in the database.

        """
        logger.info("Updating item")
        base_url = str(kwargs["request"].base_url)
        now = datetime_type.now(timezone.utc).isoformat().replace("+00:00", "Z")
        item["properties"]["updated"] = now

        # Check the owner of the specified parent collection
        collection = await self.database.find_collection(
            collection_id=collection_id, catalog_path=catalog_path
        )
        collection_owner = collection["_sfapi_internal"]["owner"]

        # Confirm that the workspace provides correct access to the part of the catalogue to be altered
        if workspace != collection_owner:
            raise HTTPException(
                status_code=403,
                detail=f"Workspace {workspace} does not have access to parent Collection {collection_id} at {catalog_path if catalog_path else 'top-level'} catalog",
            )

        # Note, if the provided item is not valid stac, this may delete the item and then fail to create the new one

        await self.database.check_collection_exists(
            collection_id=collection_id, catalog_path=catalog_path
        )
        await self.delete_item(
            item_id=item_id,
            collection_id=collection_id,
            catalog_path=catalog_path,
            workspace=workspace,
        )
        await self.create_item(
            catalog_path=catalog_path,
            collection_id=collection_id,
            item=item,
            workspace=workspace,
            **kwargs,
        )

        return ItemSerializer.db_to_stac(
            catalog_path=catalog_path, item=item, base_url=base_url
        )

    @overrides
    async def delete_item(
        self,
        item_id: str,
        collection_id: str,
        catalog_path: str,
        workspace: str,
        **kwargs,
    ) -> Optional[stac_types.Item]:
        """Delete an item from a collection.

        Args:
            item_id (str): The identifier of the item to delete.
            collection_id (str): The identifier of the collection that contains the item.

        Returns:
            Optional[stac_types.Item]: The deleted item, or `None` if the item was successfully deleted.
        """
        logger.info("Deleting item")

        # Confirm user has access to this part of the catalog
        # Retrive collection to confirm owning workspace
        collection = await self.database.find_collection(
            catalog_path=catalog_path, collection_id=collection_id
        )
        owner = collection["_sfapi_internal"]["owner"]

        if owner != workspace:
            raise HTTPException(
                status_code=403,
                detail="Workspace does not have access to the parent Collection"
            )

        await self.database.delete_item(
            item_id=item_id, collection_id=collection_id, catalog_path=catalog_path
        )
        return None

    @overrides
    async def create_collection(
        self,
        catalog_path: str,
        collection: stac_types.Collection,
        workspace: str,
        **kwargs,
    ) -> stac_types.Collection:
        """Create a new collection in the database.

        Args:
            catalog_path (str): The path to the catalog containing the collection.
            collection (stac_types.Collection): The collection to be created.
            username_header (dict): X-Username header from the request.
            workspace (str): The workspace being used to create the collection.
            is_public (bool): Whether the collection is public or not.
            **kwargs: Additional keyword arguments.

        Returns:
            stac_types.Collection: The created collection object.

        Raises:
            ConflictError: If the collection already exists.
        """
        logger.info("Creating collection")

        # Handle case where no collection is provided
        if not collection:
            raise HTTPException(status_code=400, detail="No collection provided")
        
        # Only default workspace can alter top-level catalogs
        if not catalog_path and workspace != "default_workspace":
            HTTPException(
                status_code=403,
                detail=f"Workspace does not have access to top-level catalog",
            )

        # Determine public setting
        if workspace == "default_workspace":
            is_public = True
        else:
            is_public = False

        base_url = str(kwargs["request"].base_url)

        # Confirm user has access to parent catalog
        # Retrive catalog to confirm owning workspace
        parent_catalog = await self.database.find_catalog(catalog_path=catalog_path)
        owner = parent_catalog["_sfapi_internal"]["owner"]

        if owner != workspace:
            raise HTTPException(
                status_code=403, detail="Workspace does not have access to this Catalog"
            )
        
        # If the ingester is attempting to create a private entry in a user workspace, this might need to be set public based on the parent catalog access control
        if parent_catalog and not is_public and parent_catalog["_sfapi_internal"].get("rec_public", False):
            is_public = True

        collection = self.database.collection_serializer.stac_to_db(
            collection, base_url
        )
        collection = await self.database.prep_create_collection(
            catalog_path=catalog_path, collection=collection, base_url=base_url
        )
        await self.database.create_collection(
            catalog_path=catalog_path,
            collection=collection,
            owner=workspace,
            is_public=is_public,
        )
        return CollectionSerializer.db_to_stac(
            catalog_path=catalog_path, collection=collection, base_url=base_url
        )

    @overrides
    async def update_collection(
        self,
        catalog_path: str,
        collection_id: str,
        collection: stac_types.Collection,
        workspace: str,
        **kwargs,
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
            collection: A STAC collection that needs to be updated.
            kwargs: Additional keyword arguments.

        Returns:
            A STAC collection that has been updated in the database.

        """
        logger.info("Updating collection")
        base_url = str(kwargs["request"].base_url)

        # Check the owner of the specified collection
        old_collection = await self.database.find_collection(
            collection_id=collection_id, catalog_path=catalog_path
        )
        collection_owner = old_collection["_sfapi_internal"]["owner"]
        is_public = old_collection["_sfapi_internal"]["inf_public"]

        # Confirm that the workspace provides correct access to the part of the catalogue to be altered
        if workspace != collection_owner:
            raise HTTPException(
                status_code=403,
                detail=f"Workspace {workspace} does not have access to {catalog_path if catalog_path else 'top-level'} catalog",
            )
        owner = collection_owner

        collection = self.database.collection_serializer.stac_to_db(
            collection, base_url
        )
        await self.database.update_collection(
            catalog_path=catalog_path,
            collection_id=collection_id,
            collection=collection,
            owner=owner,
            is_public=is_public,
        )

        return CollectionSerializer.db_to_stac(
            catalog_path=catalog_path, collection=collection, base_url=base_url
        )

    @overrides
    async def delete_collection(
        self, catalog_path: str, collection_id: str, workspace: str, **kwargs
    ) -> Optional[stac_types.Collection]:
        """
        Delete a collection.

        This method deletes an existing collection in the database.

        Args:
            catalog_path (str): The path to the catalog containing the collection.
            collection_id (str): The identifier of the collection that contains the item.
            workspace (str): The workspace of the user making the request.
            **kwargs: Additional keyword arguments.

        Returns:
            None.

        Raises:
            NotFoundError: If the collection doesn't exist.
        """
        logger.info("Deleting collection")

        # Confirm user has access to this part of the catalog
        # Retrive collection to confirm owning workspace
        collection = await self.database.find_collection(
            catalog_path=catalog_path, collection_id=collection_id
        )
        owner = collection["_sfapi_internal"]["owner"]

        if owner != workspace:
            raise HTTPException(
                status_code=403, detail="Workspace does not have access to this Catalog"
            )

        await self.database.delete_collection(
            collection_id=collection_id, catalog_path=catalog_path
        )
        return None

    async def update_catalog_access_control(
        self,
        workspace: str,
        catalog_path: str,
        access_policy: stac_types.AccessPolicy,
        **kwargs,
    ):

        # Get catalog_id from path
        catalog_path_list = catalog_path.split("/")
        catalog_id = catalog_path_list[-1]
        parent_catalog_path = (
            "/".join(catalog_path_list[:-1])
            if len(catalog_path_list) > 1
            else "top-level Catalog"
        )

        logger.info(
            "Updating access control for Catalog %s in Catalog %s",
            catalog_id,
            parent_catalog_path,
        )

        access_list = []
        is_public = access_policy.get("public", False)

        # Retrive catalog to confirm owning workspace
        catalog = await self.database.find_catalog(catalog_path=catalog_path)
        owner = catalog["_sfapi_internal"]["owner"]

        if owner != workspace:
            raise HTTPException(
                status_code=403, detail="Workspace does not have access to this Catalog"
            )

        await self.database.update_catalog_access_control(
            catalog_path=catalog_path,
            owner=owner,
            is_public=is_public,
        )

        logger.info(
            f"Updated access for Catalog {catalog_id} in Catalog {parent_catalog_path} to "
            f"be {'public' if is_public else 'private'} with access for {access_list}"
        )

    @overrides
    async def update_collection_access_control(
        self,
        workspace: str,
        collection_id: str,
        access_policy: stac_types.AccessPolicy,
        catalog_path: Optional[str] = None,
        **kwargs,
    ):

        logger.info(
            "Updating access control for Collection %s in Catalog %s",
            collection_id,
            catalog_path,
        )

        access_list = []
        is_public = access_policy.get("public", False)

        # Retrive catalog to confirm owning workspace
        collection = await self.database.find_collection(
            catalog_path=catalog_path, collection_id=collection_id
        )
        owner = collection["_sfapi_internal"]["owner"]

        if owner != workspace:
            raise HTTPException(
                status_code=403,
                detail="Workspace does not have access to this Collection",
            )

        await self.database.update_collection_access_control(
            catalog_path=catalog_path,
            collection_id=collection_id,
            owner=owner,
            is_public=is_public,
        )

        logger.info(
            f"Updated access for Collection {collection_id} in Catalog {catalog_path} to "
            f"be {'public' if is_public else 'private'} with access for {access_list}"
        )

    @overrides
    async def create_catalog(
        self,
        catalog: stac_types.Catalog,
        workspace: str,
        catalog_path: Optional[str] = None,
        **kwargs,
    ) -> stac_types.Catalog:
        """Create a new catalog in the database.

        Args:
            catalog (stac_types.Catalog): The catalog to be created.
            workspace (str): The workspace being used to create the catalog.
            catalog_path (Optional[str]): The path to the catalog to be created.
            is_public (bool): Whether the catalog is public or not.
            access_list (List[str]): List of users who have access to the catalog.
            **kwargs: Additional keyword arguments.

        Returns:
            stac_types.Catalog: The created catalog object.

        Raises:
            ConflictError: If the catalog already exists.
        """
        logger.info("Creating catalog")

        logger.info(catalog_path)

        # Handle case where no catalog is provided
        if not catalog:
            raise HTTPException(status_code=400, detail="No catalog provided")
        
        # Only default workspace can alter top-level catalogs
        if not catalog_path and workspace != "default_workspace":
            HTTPException(
                status_code=403,
                detail=f"Workspace does not have access to top-level catalog",
            )

        # Determine public setting
        if workspace == "default_workspace":
            is_public = True
        else:
            is_public = False

        base_url = str(kwargs["request"].base_url)

        # Confirm user has access to parent catalog
        # Retrive catalog to confirm owning workspace
        if catalog_path:
            parent_catalog = await self.database.find_catalog(catalog_path=catalog_path)
            owner = parent_catalog["_sfapi_internal"]["owner"]

            # If the ingester is attempting to create a private entry in a user workspace, this might need to be set public based on the parent catalog access control
            if parent_catalog and not is_public and parent_catalog["_sfapi_internal"].get("rec_public", False):
                is_public = True

        else:
            owner = "default_workspace"

        if owner != workspace:
            if owner != "default_workspace":
                raise HTTPException(
                    status_code=403, detail="Workspace does not have access to this Catalog"
                )

        catalog = self.database.catalog_serializer.stac_to_db(
            catalog=catalog, base_url=base_url
        )
        catalog = await self.database.prep_create_catalog(
            catalog_path=catalog_path, catalog=catalog, base_url=base_url
        )
        await self.database.create_catalog(
            catalog_path=catalog_path,
            catalog=catalog,
            owner=workspace,
            is_public=is_public,
        )

        # This catalog does not yet have any collections or sub-catalogs
        return CatalogSerializer.db_to_stac(
            catalog_path=catalog_path,
            catalog=catalog,
            base_url=base_url,
        )  # not needed here: conformance_classes=self.conformance_classes())

    @overrides
    async def update_catalog(
        self, catalog_path: str, catalog: stac_types.Catalog, workspace: str, **kwargs
    ) -> stac_types.Catalog:
        """
        Update a collection.

        This method updates an existing collection in the database by first finding
        the collection by the id given in the keyword argument `collection_id`.
        If no `collection_id` is given the id of the given collection object is used.
        If the object and keyword collection ids don't match the sub items
        collection id is updated else the items are left unchanged.
        The updated collection is then returned.

        Args:
            collection: A STAC collection that needs to be updated.
            kwargs: Additional keyword arguments.

        Returns:
            A STAC collection that has been updated in the database.

        """
        logger.info("Updating catalog")
        base_url = str(kwargs["request"].base_url)

        # Check the owner of the specified catalog
        old_catalog = await self.database.find_catalog(catalog_path=catalog_path)
        catalog_owner = old_catalog["_sfapi_internal"]["owner"]

        # Confirm that the workspace provides correct access to the part of the catalogue to be altered
        if workspace != catalog_owner:
            raise HTTPException(
                status_code=403,
                detail=f"Workspace {workspace} does not have access to {catalog_path if catalog_path else 'top-level'} catalog",
            )

        catalog = self.database.catalog_serializer.stac_to_db(
            catalog=catalog, base_url=base_url
        )
        # Set owner and access_control to that of before
        await self.database.update_catalog(catalog_path=catalog_path, catalog=catalog, owner=workspace, is_public=is_public)

        # This catalog does not yet have any collections or sub-catalogs
        return CatalogSerializer.db_to_stac(
            catalog_path=catalog_path, catalog=catalog, base_url=base_url
        )  # not needed here: conformance_classes=self.conformance_classes())

    @overrides
    async def delete_catalog(
        self, catalog_path: str, workspace: str, **kwargs
    ) -> Optional[stac_types.Catalog]:
        """
        Delete a collection.

        This method deletes an existing collection in the database.

        Args:
            collection_id (str): The identifier of the collection that contains the item.
            kwargs: Additional keyword arguments.

        Returns:
            None.

        Raises:
            NotFoundError: If the collection doesn't exist.
        """
        logger.info("Deleting catalog")

        # Confirm user has access to this part of the catalog
        # Retrive catalog to confirm owning workspace
        catalog = await self.database.find_catalog(catalog_path=catalog_path)
        owner = catalog["_sfapi_internal"]["owner"]

        if owner != workspace:
            raise HTTPException(
                status_code=403, detail="Workspace does not have access to this Catalog"
            )

        await self.database.delete_catalog(catalog_path=catalog_path)
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
        self,
        catalog_path: str,
        items: Items,
        chunk_size: Optional[int] = None,
        **kwargs,
    ) -> str:
        """Perform a bulk insertion of items into the database using Elasticsearch.

        Args:
            items: The items to insert.
            chunk_size: The size of each chunk for bulk processing.
            **kwargs: Additional keyword arguments, such as `request` and `refresh`.

        Returns:
            A string indicating the number of items successfully added.
        """
        logger.info("Bulk inserting items")
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
            catalog_path=catalog_path,
            collection_id=collection_id,
            processed_items=processed_items,
            refresh=kwargs.get("refresh", False),
        )

        return f"Successfully added {len(processed_items)} Items."


@attr.s
class EsAsyncBaseFiltersClient(AsyncBaseFiltersClient):
    """Defines a pattern for implementing the STAC filter extension."""

    # todo: use the ES _mapping endpoint to dynamically find what fields exist
    async def get_queryables(
        self, collection_id: Optional[str] = None, **kwargs
    ) -> Dict[str, Any]:
        """Get the queryables available for the given collection_id.

        If collection_id is None, returns the intersection of all
        queryables over all collections.

        This base implementation returns a blank queryable schema. This is not allowed
        under OGC CQL but it is allowed by the STAC API Filter Extension

        https://github.com/radiantearth/stac-api-spec/tree/master/fragments/filter#queryables

        Args:
            collection_id (str, optional): The id of the collection to get queryables for.
            **kwargs: additional keyword arguments

        Returns:
            Dict[str, Any]: A dictionary containing the queryables for the given collection.
        """
        logger.info("Getting queryables")
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
class EsAsyncCollectionSearchClient(AsyncCollectionSearchClient):
    """Defines a pattern for implementing the STAC collection search extension."""

    database: BaseDatabaseLogic = attr.ib()
    post_request_model = attr.ib(default=BaseCollectionSearchPostRequest)
    collection_serializer: Type[CollectionSerializer] = attr.ib(
        default=CollectionSerializer
    )

    async def post_all_collections(
        self,
        search_request: BaseCollectionSearchPostRequest,
        request: Request,
        username_header: dict,
        catalog_path: str = None,
        **kwargs,
    ) -> Collections:
        """
        Perform a POST search on the collections in the catalog.

        Args:
            search_request (BaseCollectionSearchPostRequest): Request object that includes the parameters for the search.
            request (Request): The incoming request.
            username_header (dict): X-Username header from the request.
            catalog_path (Str): The path to the catalog in which to search the collections.
            **kwargs: Keyword arguments passed to the function.

        Returns:
            A tuple of (collections, next pagination token if any).

        Raises:
            HTTPException: If there is an error with the cql2_json filter.
        """
        logger.info("Posting for all collections")
        base_url = str(request.base_url)
        token = request.query_params.get("token")
        limit = int(request.query_params.get("limit", 10))

        # Extract X-Username header from username_header for access control
        username = username_header.get("X-Username", "")

        # Get user index
        user_index = hash_to_index(username)

        if catalog_path:
            # Get Catalog to confirm user access
            catalog = await self.database.find_catalog(catalog_path=catalog_path)

            # Extract X-Username header from username_header for access control
            username = username_header.get("X-Username", "")

            # Get user index
            user_index = hash_to_index(username)

            # Get access control array for each catalog
            try:
                # Check access control
                if not catalog["_sfapi_internal"]["owner"] == username:
                    if not catalog["_sfapi_internal"]["inf_public"]:  # Catalog is private
                        if username == "":
                            raise HTTPException(
                                status_code=401, detail="User is not authenticated"
                            )
                        else:
                            raise HTTPException(
                                status_code=403,
                                detail="User does not have access to this catalog",
                            )
            except KeyError:
                logger.error(f"Access control not found for catalog {catalog['id']}")
                if username == "":
                    raise HTTPException(
                        status_code=401, detail="User is not authenticated"
                    )
                else:
                    raise HTTPException(
                        status_code=403,
                        detail="User does not have access to this catalog",
                    )

        search = self.database.make_collection_search()

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

        # q is a list of keywords
        if search_request.q:
            q = search_request.q
            search = self.database.apply_keyword_collections_filter(search=search, q=q)

        sort = None

        limit = 10
        if search_request.limit:
            limit = search_request.limit

        collections, _, next_token = (
            await self.database.execute_collection_search(
                search=search,
                limit=limit,
                base_url=base_url,
                token=token,
                sort=sort,
                catalog_path=catalog_path,
            )
        )

        links = []
        if next_token:
            links = await PagingLinks(request=request, next=next_token).get_links()

        return Collections(collections=collections, links=links)

    # todo: use the ES _mapping endpoint to dynamically find what fields exist
    async def get_all_collections(
        self,
        request: Request,
        username_header: dict,
        catalog_path: str = None,
        bbox: Optional[List[NumType]] = None,
        datetime: Optional[Union[str, datetime_type]] = None,
        limit: Optional[int] = 10,
        q: Optional[List[str]] = None,
        **kwargs,
    ) -> Collections:
        """Get search results from the database for collections.
        Called with `GET /collection-search`.
        Args:
            request (Request): The incoming request.
            username_header (dict): X-Username header from the request.
            bbox (Optional[List[NumType]]): Bounding box to search in.
            datetime (Optional[Union[str, datetime_type]]): Filter items based on the datetime field.
            limit (Optional[int]): Maximum number of results to return.
            q (Optional[List[str]]): Query string to filter the results.
            kwargs: Additional parameters to be passed to the API.

        Returns:
            A tuple of (collections, next pagination token if any).

        Raises:
            HTTPException: If any error occurs while searching the catalog.
        """
        logger.info("Getting all collections")

        token = request.query_params.get("token")

        base_args = {
            "limit": limit,
            "token": token,
            "bbox": bbox,
            "datetime": datetime,
            "q": q,
        }

        # Do the request
        try:
            search_request = self.post_request_model(**base_args)
        except ValidationError:
            raise HTTPException(status_code=400, detail="Invalid parameters provided")
        resp = await self.post_all_collections(
            search_request=search_request,
            request=request,
            username_header=username_header,
            catalog_path=catalog_path,
        )

        return resp


@attr.s
class EsAsyncDiscoverySearchClient(AsyncDiscoverySearchClient):
    """Defines a pattern for implementing the STAC collection search extension."""

    database: BaseDatabaseLogic = attr.ib()
    post_request_model = attr.ib(default=BaseDiscoverySearchPostRequest)
    catalog_collection_serializer: Type[CatalogCollectionSerializer] = attr.ib(
        default=CatalogCollectionSerializer
    )

    extensions: List[ApiExtension] = attr.ib(default=attr.Factory(list))
    base_conformance_classes: List[str] = attr.ib(
        factory=lambda: BASE_CONFORMANCE_CLASSES
    )

    def conformance_classes(self) -> List[str]:
        """Generate conformance classes by adding extension conformance to base
        conformance classes."""
        base_conformance_classes = self.base_conformance_classes.copy()

        for extension in self.extensions:
            extension_classes = getattr(extension, "conformance_classes", [])
            base_conformance_classes.extend(extension_classes)

        return list(set(base_conformance_classes))

    async def post_discovery_search(
        self,
        search_request: BaseDiscoverySearchPostRequest,
        request: Request,
        username_header: dict,
        **kwargs,
    ) -> Collections:
        """
        Perform a POST search on the collections in the catalog.

        Args:
            search_request (BaseCollectionSearchPostRequest): Request object that includes the parameters for the search.
            request (Request): The incoming request.
            username_header (dict): X-Username header from the request.
            **kwargs: Keyword arguments passed to the function.

        Returns:
            A tuple of (collections, next pagination token if any).

        Raises:
            HTTPException: If there is an error with the cql2_json filter.
        """
        logger.info("Posting a discovery search")
        base_url = str(request.base_url)
        token = request.query_params.get("token")
        limit = int(request.query_params.get("limit", 10))

        # Extract X-Username header from username_header for access control
        username = username_header.get("X-Username", "")

        # Get user index
        user_index = hash_to_index(username)

        search = self.database.make_discovery_search()

        if search_request.q:
            q = search_request.q
            search = self.database.apply_keyword_discovery_filter(search=search, q=q)

        # TODO: Need to get pagination working with sorting by score, current sorting will be by default instead
        # sort = [
        #     {"_score": {"order": "desc"}},
        # ]

        catalogs_and_collections = []

        catalogs_and_collections, _, next_token = (
            await self.database.execute_discovery_search(
                search=search,
                limit=limit,
                token=token,
                sort=None,  # use default sort for the minute
                base_url=base_url,
                conformance_classes=self.conformance_classes(),
            )
        )

        links = []
        if next_token:
            links = await PagingLinks(request=request, next=next_token).get_links()

        return CatalogsAndCollections(
            catalogs_and_collections=catalogs_and_collections, links=links
        )

    # todo: use the ES _mapping endpoint to dynamically find what fields exist
    async def get_discovery_search(
        self,
        request: Request,
        username_header: dict,
        q: Optional[List[str]] = None,
        limit: Optional[int] = 10,
        **kwargs,
    ) -> Collections:
        """Get search results from the database for catalogues.
        Called with `GET /catalogues`.
        Args:
            request (Request): The incoming request.
            username_header (dict): X-Username header from the request.
            q (Optional[List[str]]): Query string to filter the results.
            limit (Optional[int]): Maximum number of results to return.
            kwargs: Additional parameters to be passed to the API.

        Returns:
            A tuple of (collections, next pagination token if any).

        Raises:
            HTTPException: If any error occurs while searching the catalog.
        """
        logger.info("Getting discovery search")

        base_args = {
            "q": q,
            "limit": limit,
        }

        try:
            search_request = self.post_request_model(**base_args)
        except ValidationError:
            raise HTTPException(status_code=400, detail="Invalid parameters provided")
        resp = await self.post_discovery_search(
            search_request=search_request,
            request=request,
            username_header=username_header,
        )

        return resp
