"""Serializers."""
import abc
from copy import deepcopy
from typing import Any, List, Optional, Tuple

import attr
from starlette.requests import Request

from stac_fastapi.core.datetime_utils import now_to_rfc3339_str
from stac_fastapi.core.models.links import CollectionLinks, CatalogLinks
from stac_fastapi.types import stac as stac_types
from stac_fastapi.types.links import ItemLinks, resolve_links
from stac_fastapi.types.conformance import BASE_CONFORMANCE_CLASSES

from urllib.parse import urljoin
from stac_pydantic.shared import MimeTypes

def regen_cat_path(cat_path: str) -> Tuple[str, str]:
        if cat_path == "root":
            return ""
        cat_path = cat_path.replace(",", "/catalogs/")
        return cat_path

@attr.s
class Serializer(abc.ABC):
    """Defines serialization methods between the API and the data model.

    This class is meant to be subclassed and implemented by specific serializers for different STAC objects (e.g. Item, Collection).
    """

    @classmethod
    @abc.abstractmethod
    def db_to_stac(cls, item: dict, base_url: str) -> Any:
        """Transform database model to STAC object.

        Arguments:
            item (dict): A dictionary representing the database model.
            base_url (str): The base URL of the STAC API.

        Returns:
            Any: A STAC object, e.g. an `Item` or `Collection`, representing the input `item`.
        """
        ...

    @classmethod
    @abc.abstractmethod
    def stac_to_db(cls, stac_object: Any, base_url: str) -> dict:
        """Transform STAC object to database model.

        Arguments:
            stac_object (Any): A STAC object, e.g. an `Item` or `Collection`.
            base_url (str): The base URL of the STAC API.

        Returns:
            dict: A dictionary representing the database model.
        """
        ...


class ItemSerializer(Serializer):
    """Serialization methods for STAC items."""

    @classmethod
    def stac_to_db(cls, stac_data: stac_types.Item, base_url: str) -> stac_types.Item:
        """Transform STAC item to database-ready STAC item.

        Args:
            stac_data (stac_types.Item): The STAC item object to be transformed.
            base_url (str): The base URL for the STAC API.

        Returns:
            stac_types.Item: The database-ready STAC item object.
        """
        item_links = resolve_links(stac_data.get("links", []), base_url)
        stac_data["links"] = item_links

        now = now_to_rfc3339_str()
        if "created" not in stac_data["properties"]:
            stac_data["properties"]["created"] = now
        stac_data["properties"]["updated"] = now
        return stac_data

    @classmethod
    def db_to_stac(cls, item: dict, base_url: str) -> stac_types.Item:
        """Transform database-ready STAC item to STAC item.

        Args:
            item (dict): The database-ready STAC item to be transformed.
            base_url (str): The base URL for the STAC API.

        Returns:
            stac_types.Item: The STAC item object.
        """
        item_id = item["id"]
        collection_id = item["collection"]
        cat_path = regen_cat_path(item.get("_sfapi_internal", {}).get("cat_path", ""))
        item_links = ItemLinks(
            catalog_path=cat_path, collection_id=collection_id, item_id=item_id, base_url=base_url
        ).create_links()

        original_links = item.get("links", [])
        if original_links:
            item_links += resolve_links(original_links, base_url)

        return stac_types.Item(
            type="Feature",
            stac_version=item.get("stac_version", ""),
            stac_extensions=item.get("stac_extensions", []),
            id=item_id,
            collection=item.get("collection", ""),
            geometry=item.get("geometry", {}),
            bbox=item.get("bbox", []),
            properties=item.get("properties", {}),
            links=item_links,
            assets=item.get("assets", {}),
        )


class CollectionSerializer(Serializer):
    """Serialization methods for STAC collections."""

    @classmethod
    def stac_to_db(
        cls, collection: stac_types.Collection, request: Request
    ) -> stac_types.Collection:
        """
        Transform STAC Collection to database-ready STAC collection.

        Args:
            stac_data: the STAC Collection object to be transformed
            starlette.requests.Request: the API request

        Returns:
            stac_types.Collection: The database-ready STAC Collection object.
        """
        collection = deepcopy(collection)
        collection["links"] = resolve_links(
            collection.get("links", []), str(request.base_url)
        )
        return collection

    @classmethod
    def db_to_stac(
        cls, collection: dict, request: Request, extensions: Optional[List[str]] = []
    ) -> stac_types.Collection:
        """Transform database model to STAC collection.

        Args:
            collection (dict): The collection data in dictionary form, extracted from the database.
            starlette.requests.Request: the API request
            extensions: A list of the extension class names (`ext.__name__`) or all enabled STAC API extensions.

        Returns:
            stac_types.Collection: The STAC collection object.
        """
        # Avoid modifying the input dict in-place ... doing so breaks some tests
        collection = deepcopy(collection)

        # Set defaults
        collection_id = collection.get("id")
        collection.setdefault("type", "Collection")
        collection.setdefault("stac_extensions", [])
        collection.setdefault("stac_version", "")
        collection.setdefault("title", "")
        collection.setdefault("description", "")
        collection.setdefault("keywords", [])
        collection.setdefault("license", "")
        collection.setdefault("providers", [])
        collection.setdefault("summaries", {})
        collection.setdefault(
            "extent", {"spatial": {"bbox": []}, "temporal": {"interval": []}}
        )
        collection.setdefault("assets", {})

        # Create the collection links using CollectionLinks
        cat_path = regen_cat_path(collection.get("_sfapi_internal", {}).get("cat_path", ""))
        collection_links = CollectionLinks(
            catalog_path=cat_path, collection_id=collection_id, request=request, extensions=extensions
        ).create_links()

        # Add any additional links from the collection dictionary
        original_links = collection.get("links")
        if original_links:
            collection_links += resolve_links(original_links, str(request.base_url))
        collection["links"] = collection_links

        # Pop any unnecessary keys
        collection.pop("_sfapi_internal", None)

        # Return the stac_types.Collection object
        return stac_types.Collection(**collection)
    

class CatalogSerializer(Serializer):
    """Serialization methods for STAC catalogs."""

    @classmethod
    def stac_to_db(
        cls, catalog: stac_types.Catalog, request: Request
    ) -> stac_types.Catalog:
        """
        Transform STAC Catalog to database-ready STAC catalog.

        Args:
            stac_data: the STAC Catalog object to be transformed
            starlette.requests.Request: the API request

        Returns:
            stac_types.Catalog: The database-ready STAC Catalog object.
        """
        catalog = deepcopy(catalog)
        catalog["links"] = resolve_links(
            catalog.get("links", []), str(request.base_url)
        )
        return catalog

    @classmethod
    def db_to_stac(
        cls, catalog: dict, request: Request, sub_catalogs: List[str] = [], sub_collections: List[str] = [], conformance_classes: List[str] = BASE_CONFORMANCE_CLASSES, extensions: Optional[List[str]] = []
    ) -> stac_types.Catalog:
        """Transform database model to STAC catalog.

        Args:
            catalog (dict): The catalog data in dictionary form, extracted from the database.
            starlette.requests.Request: the API request
            extensions: A list of the extension class names (`ext.__name__`) or all enabled STAC API extensions.

        Returns:
            stac_types.Catalog: The STAC catalog object.
        """
        # Avoid modifying the input dict in-place ... doing so breaks some tests
        catalog = deepcopy(catalog)

        # Set defaults
        catalog_id = catalog.get("id")
        catalog.setdefault("type", "Catalog")
        catalog.setdefault("stac_extensions", [])
        catalog.setdefault("stac_version", "")
        catalog.setdefault("title", "")
        catalog.setdefault("description", "")
        catalog.setdefault("conformsTo", conformance_classes)

        # Create the catalog links using CatalogLinks
        cat_path = regen_cat_path(catalog.get("_sfapi_internal", {}).get("cat_path", ""))
        catalog_links = CatalogLinks(
            catalog_path=cat_path, catalog_id=catalog_id, request=request, extensions=extensions
        ).create_links()

        # Add any additional links from the catalog dictionary
        original_links = catalog.get("links")
        if original_links:
            catalog_links += resolve_links(original_links, str(request.base_url))
        catalog["links"] = catalog_links

        # Add sub catalog and collection links
        for sub_catalog in sub_catalogs:
            if cat_path:
                href_url = "catalogs/" + cat_path + "/"
            else:
                href_url = ""
            catalog["links"].append(
                {
                    "rel": "child",
                    "type": MimeTypes.json.value,
                    "href": urljoin(str(request.base_url), f"{href_url}catalogs/{catalog_id}/catalogs/{sub_catalog[0]}"),
                    "title": sub_catalog[1],
                }
            )
        for sub_collection in sub_collections:
            if cat_path:
                href_url = "catalogs/" + cat_path + "/"
            else:
                href_url = ""
            catalog["links"].append(
                {
                    "rel": "child",
                    "type": MimeTypes.json.value,
                    "href": urljoin(str(request.base_url), f"{href_url}catalogs/{catalog_id}/collections/{sub_collection[0]}"),
                    "title": sub_collection[1],
                }
            )

        # Pop any unnecessary keys
        catalog.pop("_sfapi_internal", None)

        # Return the stac_types.Catalog object
        return stac_types.Catalog(**catalog)
