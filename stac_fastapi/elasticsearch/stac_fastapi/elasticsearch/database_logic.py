"""Database logic."""

import asyncio
import json
import logging
import os
from base64 import urlsafe_b64decode, urlsafe_b64encode
from copy import deepcopy
from typing import Any, Dict, Iterable, List, Optional, Protocol, Tuple, Type, Union

import attr
from elasticsearch_dsl import Q, Search
from starlette.requests import Request

from elasticsearch import exceptions, helpers  # type: ignore
from stac_fastapi.core.extensions import filter
from stac_fastapi.core.serializers import CatalogSerializer, CollectionSerializer, ItemSerializer
from stac_fastapi.core.utilities import MAX_LIMIT, bbox2polygon
from stac_fastapi.elasticsearch.config import AsyncElasticsearchSettings
from stac_fastapi.elasticsearch.config import (
    ElasticsearchSettings as SyncElasticsearchSettings,
)
from stac_fastapi.types.errors import ConflictError, NotFoundError, UnauthorizedError, UnauthenticatedError
from stac_fastapi.types.stac import Catalog, Collection, Item

logger = logging.getLogger(__name__)

NumType = Union[float, int]

ADMIN_WORKSPACE = os.getenv("ADMIN_WORKSPACE", "default_workspace")
USER_WRITEABLE_CATALOG = os.getenv("USER_WRITEABLE_CATALOG", "user-datasets")

CATALOGS_INDEX = os.getenv("STAC_CATALOGS_INDEX", "catalogs")
COLLECTIONS_INDEX = os.getenv("STAC_COLLECTIONS_INDEX", "collections")
ITEMS_INDEX = os.getenv("STAC_ITEMS_INDEX", "items")
ITEMS_INDEX_PREFIX = os.getenv("STAC_ITEMS_INDEX_PREFIX", "items_")
ES_INDEX_NAME_UNSUPPORTED_CHARS = {
    "\\",
    "/",
    "*",
    "?",
    '"',
    "<",
    ">",
    "|",
    " ",
    ",",
    "#",
    ":",
}

ITEM_INDICES = f"{ITEMS_INDEX}*,-*kibana*,-{COLLECTIONS_INDEX}*"

DEFAULT_SORT = {
    "properties.datetime": {"order": "desc"},
    "id": {"order": "desc"},
    "collection": {"order": "desc"},
}

ES_ITEMS_SETTINGS = {
    "index": {
        "sort.field": list(DEFAULT_SORT.keys()),
        "sort.order": [v["order"] for v in DEFAULT_SORT.values()],
    },
    "analysis": {
        "tokenizer": {
            "edge_ngram_tokenizer": {
                "type": "edge_ngram",
                "min_gram": 1,
                "max_gram": 20,
                "token_chars": ["letter", "digit"],
            }
        },
        "analyzer": {
            "edge_ngram_analyzer": {
                "type": "custom",
                "tokenizer": "edge_ngram_tokenizer",
            }
        }
    }
}

ES_COLLECTIONS_SETTINGS = {
    "analysis": {
        "tokenizer": {
            "edge_ngram_tokenizer": {
                "type": "edge_ngram",
                "min_gram": 1,
                "max_gram": 20,
                "token_chars": ["letter", "digit"],
            }
        },
        "analyzer": {
            "edge_ngram_analyzer": {
                "type": "custom",
                "tokenizer": "edge_ngram_tokenizer",
            }
        }
    }
}

ES_MAPPINGS_DYNAMIC_TEMPLATES = [
    # Common https://github.com/radiantearth/stac-spec/blob/master/item-spec/common-metadata.md
    {
        "descriptions": {
            "match_mapping_type": "string",
            "match": "description",
            "mapping": {"type": "text"},
        }
    },
    {
        "titles": {
            "match_mapping_type": "string",
            "match": "title",
            "mapping": {"type": "text"},
        }
    },
    # Projection Extension https://github.com/stac-extensions/projection
    {"proj_epsg": {"match": "proj:epsg", "mapping": {"type": "integer"}}},
    {
        "proj_projjson": {
            "match": "proj:projjson",
            "mapping": {"type": "object", "enabled": False},
        }
    },
    {
        "proj_centroid": {
            "match": "proj:centroid",
            "mapping": {"type": "geo_point"},
        }
    },
    {
        "proj_geometry": {
            "match": "proj:geometry",
            "mapping": {"type": "object", "enabled": False},
        }
    },
    {
        "no_index_href": {
            "match": "href",
            "mapping": {"type": "text", "index": False},
        }
    },
    # Default all other strings not otherwise specified to keyword
    {"strings": {"match_mapping_type": "string", "mapping": {"type": "keyword"}}},
    {"numerics": {"match_mapping_type": "long", "mapping": {"type": "float"}}},
]

ES_ITEMS_MAPPINGS = {
    "numeric_detection": False,
    "dynamic_templates": ES_MAPPINGS_DYNAMIC_TEMPLATES,
    "properties": {
        "id": {"type": "keyword"},
        "collection": {"type": "keyword"},
        "geometry": {"type": "geo_shape"},
        "assets": {"type": "object", "enabled": False},
        "links": {"type": "object", "enabled": False},
        "properties": {
            "type": "object",
            "properties": {
                # Common https://github.com/radiantearth/stac-spec/blob/master/item-spec/common-metadata.md
                "datetime": {"type": "date"},
                "start_datetime": {"type": "date"},
                "end_datetime": {"type": "date"},
                "created": {"type": "date"},
                "updated": {"type": "date"},
                # Satellite Extension https://github.com/stac-extensions/sat
                "sat:absolute_orbit": {"type": "integer"},
                "sat:relative_orbit": {"type": "integer"},
            },
        },
        "_sfapi_internal": {
            "type": "object",
            "properties": {
                "cat_path": {"type": "keyword"},
                "owner": {"type": "keyword"},
                "public": {"type": "boolean"},
            }
            # "analyzer": "edge_ngram_analyzer",
        }
    },
}

ES_COLLECTIONS_MAPPINGS = {
    "numeric_detection": False,
    "dynamic_templates": ES_MAPPINGS_DYNAMIC_TEMPLATES,
    "properties": {
        "id": {"type": "keyword"},
        "extent.spatial.bbox": {"type": "long"},
        "extent.temporal.interval": {"type": "date"},
        "providers": {"type": "object", "enabled": False},
        "links": {"type": "object", "enabled": False},
        "item_assets": {"type": "object", "enabled": False},
        "_sfapi_internal": {
            "type": "object",
            "properties": {
                "cat_path": {"type": "keyword"},
                "owner": {"type": "keyword"},
                "public": {"type": "boolean"},
            }
            # "analyzer": "edge_ngram_analyzer",
        }
    },
}


def index_by_collection_id(collection_id: str) -> str:
    """
    Translate a collection id into an Elasticsearch index name.

    Args:
        collection_id (str): The collection id to translate into an index name.

    Returns:
        str: The index name derived from the collection id.
    """
    return f"{ITEMS_INDEX_PREFIX}{''.join(c for c in collection_id.lower() if c not in ES_INDEX_NAME_UNSUPPORTED_CHARS)}"


def indices(collection_ids: Optional[List[str]]) -> str:
    """
    Get a comma-separated string of index names for a given list of collection ids.

    Args:
        collection_ids: A list of collection ids.

    Returns:
        A string of comma-separated index names. If `collection_ids` is None, returns the default indices.
    """
    if collection_ids is None or collection_ids == []:
        return ITEM_INDICES
    else:
        return ",".join([index_by_collection_id(c) for c in collection_ids])


async def create_index_templates() -> None:
    """
    Create index templates for the Collection and Item indices.

    Returns:
        None

    """
    client = AsyncElasticsearchSettings().create_client
    await client.indices.put_template(
        name=f"template_{CATALOGS_INDEX}",
        body={
            "index_patterns": [f"{CATALOGS_INDEX}*"],
            "settings": ES_COLLECTIONS_SETTINGS,
            "mappings": ES_COLLECTIONS_MAPPINGS,
        },
    )
    await client.indices.put_template(
        name=f"template_{COLLECTIONS_INDEX}",
        body={
            "index_patterns": [f"{COLLECTIONS_INDEX}*"],
            "settings": ES_COLLECTIONS_SETTINGS,
            "mappings": ES_COLLECTIONS_MAPPINGS,
        },
    )
    await client.indices.put_template(
        name=f"template_{ITEMS_INDEX}",
        body={
            "index_patterns": [f"{ITEMS_INDEX}*"],
            "settings": ES_ITEMS_SETTINGS,
            "mappings": ES_ITEMS_MAPPINGS,
        },
    )
    await client.close()


async def create_catalog_index() -> None:
    """
    Create the index for a Catalog. The settings of the index template will be used implicitly.

    Returns:
        None

    """
    client = AsyncElasticsearchSettings().create_client

    await client.options(ignore_status=400).indices.create(
        index=f"{CATALOGS_INDEX}-000001",
        aliases={CATALOGS_INDEX: {}},
    )
    await client.close()

async def create_collection_index() -> None:
    """
    Create the index for a Collection. The settings of the index template will be used implicitly.

    Returns:
        None

    """
    client = AsyncElasticsearchSettings().create_client

    await client.options(ignore_status=400).indices.create(
        index=f"{COLLECTIONS_INDEX}-000001",
        aliases={COLLECTIONS_INDEX: {}},
    )
    await client.close()


async def create_item_index(collection_id: str=None):
    """
    Create the index for Items. The settings of the index template will be used implicitly.

    Args:
        collection_id (str): Collection identifier.

    Returns:
        None

    """
    client = AsyncElasticsearchSettings().create_client
    # index_name = index_by_collection_id(collection_id)

    # await client.options(ignore_status=400).indices.create(
    #     index=f"{index_by_collection_id(collection_id)}-000001",
    #     aliases={index_name: {}},
    # )
    await client.options(ignore_status=400).indices.create(
        index=f"{ITEMS_INDEX}-000001",
        aliases={ITEMS_INDEX: {}},
    )
    await client.close()


async def delete_item_index(collection_id: str):
    """Delete the index for items in a collection.

    Args:
        collection_id (str): The ID of the collection whose items index will be deleted.
    """
    client = AsyncElasticsearchSettings().create_client

    name = index_by_collection_id(collection_id)
    resolved = await client.indices.resolve_index(name=name)
    if "aliases" in resolved and resolved["aliases"]:
        [alias] = resolved["aliases"]
        await client.indices.delete_alias(index=alias["indices"], name=alias["name"])
        await client.indices.delete(index=alias["indices"])
    else:
        await client.indices.delete(index=name)
    await client.close()


async def delete_collection_index(collection_id: str):
    """Delete the index for items in a collection.

    Args:
        collection_id (str): The ID of the collection whose items index will be deleted.
    """
    client = AsyncElasticsearchSettings().create_client

    name = index_by_collection_id(collection_id)
    resolved = await client.indices.resolve_index(name=name)
    if "aliases" in resolved and resolved["aliases"]:
        [alias] = resolved["aliases"]
        await client.indices.delete_alias(index=alias["indices"], name=alias["name"])
        await client.indices.delete(index=alias["indices"])
    else:
        await client.indices.delete(index=name)
    await client.close()

async def delete_catalogs_by_id_prefix(prefix: str, refresh: bool = True):
    print(f"Deleting catalogs with cat_path {prefix}")
    client = AsyncElasticsearchSettings().create_client
    pattern = f"{prefix}*"
    body={
        "query": {
            "wildcard": {
                "_sfapi_internal.cat_path": {
                    "value": pattern
                }
            }
        }
    }
    print(body)
    response = await client.delete_by_query(
        index=CATALOGS_INDEX,
        body=body,
        refresh=refresh  # Ensure the index is refreshed after deletion
    )

    # Print the number of collections deleted
    deleted_count = response.get('deleted', 0)
    print(f"Number of catalogs deleted: {deleted_count}")

async def delete_collections_by_id_prefix(prefix: str, refresh: bool = True):
    print(f"Deleting collection with cat_path {prefix}")
    client = AsyncElasticsearchSettings().create_client
    pattern = f"{prefix}*"
    body={
        "query": {
            "wildcard": {
                "_sfapi_internal.cat_path": {
                    "value": pattern
                }
            }
        }
    }
    response = await client.delete_by_query(
        index=COLLECTIONS_INDEX,
        body=body,
        refresh=refresh  # Ensure the index is refreshed after deletion
    )

    # Print the number of collections deleted
    deleted_count = response.get('deleted', 0)
    print(f"Number of collections deleted: {deleted_count}")

async def delete_items_by_id_prefix(prefix: str, refresh: bool = True):
    print(f"Deleting items with cat_path {prefix}")
    client = AsyncElasticsearchSettings().create_client
    pattern = f"{prefix}*"
    body={
        "query": {
            "wildcard": {
                "_sfapi_internal.cat_path": {
                    "value": pattern
                }
            }
        }
    }
    response = await client.delete_by_query(
        index=ITEMS_INDEX,
        body=body,
        refresh=refresh  # Ensure the index is refreshed after deletion
    )
    # Print the number of items deleted
    deleted_count = response.get('deleted', 0)
    print(f"Number of items deleted: {deleted_count}")

async def delete_items_by_id_prefix_and_collection(prefix: str, collection_id: str, refresh: bool = True):
    print(f"Deleting items with cat_path {prefix} and collection_id {collection_id}")
    client = AsyncElasticsearchSettings().create_client
    body = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"_sfapi_internal.cat_path": prefix}},
                    {"term": {"collection": collection_id}}
                ]
            } 
        }    
    }
    response = await client.delete_by_query(
        index=ITEMS_INDEX,
        body=body,
        refresh=refresh  # Ensure the index is refreshed after deletion
    )
    # Print the number of items deleted
    deleted_count = response.get('deleted', 0)
    print(f"Number of items deleted: {deleted_count}")


def mk_item_id(item_id: str, collection_id: str):
    """Create the document id for an Item in Elasticsearch.

    Args:
        item_id (str): The id of the Item.
        collection_id (str): The id of the Collection that the Item belongs to.

    Returns:
        str: The document id for the Item, combining the Item id and the Collection id, separated by a `|` character.
    """
    return f"{item_id}|{collection_id}"


def mk_actions(collection_id: str, processed_items: List[Item]):
    """Create Elasticsearch bulk actions for a list of processed items.

    Args:
        collection_id (str): The identifier for the collection the items belong to.
        processed_items (List[Item]): The list of processed items to be bulk indexed.

    Returns:
        List[Dict[str, Union[str, Dict]]]: The list of bulk actions to be executed,
        each action being a dictionary with the following keys:
        - `_index`: the index to store the document in.
        - `_id`: the document's identifier.
        - `_source`: the source of the document.
    """
    return [
        {
            "_index": index_by_collection_id(collection_id),
            "_id": mk_item_id(item["id"], item["collection"]),
            "_source": item,
        }
        for item in processed_items
    ]


# stac_pydantic classes extend _GeometryBase, which doesn't have a type field,
# So create our own Protocol for typing
# Union[ Point, MultiPoint, LineString, MultiLineString, Polygon, MultiPolygon, GeometryCollection]
class Geometry(Protocol):  # noqa
    type: str
    coordinates: Any


@attr.s
class DatabaseLogic:
    """Database logic."""

    client = AsyncElasticsearchSettings().create_client
    sync_client = SyncElasticsearchSettings().create_client

    item_serializer: Type[ItemSerializer] = attr.ib(default=ItemSerializer)
    collection_serializer: Type[CollectionSerializer] = attr.ib(
        default=CollectionSerializer
    )
    catalog_serializer: Type[CatalogSerializer] = attr.ib(default=CatalogSerializer)

    extensions: List[str] = attr.ib(default=attr.Factory(list))

    aggregation_mapping: Dict[str, Dict[str, Any]] = {
        "total_count": {"value_count": {"field": "id"}},
        "collection_frequency": {"terms": {"field": "collection", "size": 100}},
        "platform_frequency": {"terms": {"field": "properties.platform", "size": 100}},
        "cloud_cover_frequency": {
            "range": {
                "field": "properties.eo:cloud_cover",
                "ranges": [
                    {"to": 5},
                    {"from": 5, "to": 15},
                    {"from": 15, "to": 40},
                    {"from": 40},
                ],
            }
        },
        "datetime_frequency": {
            "date_histogram": {
                "field": "properties.datetime",
                "calendar_interval": "month",
            }
        },
        "datetime_min": {"min": {"field": "properties.datetime"}},
        "datetime_max": {"max": {"field": "properties.datetime"}},
        "grid_code_frequency": {
            "terms": {
                "field": "properties.grid:code",
                "missing": "none",
                "size": 10000,
            }
        },
        "sun_elevation_frequency": {
            "histogram": {"field": "properties.view:sun_elevation", "interval": 5}
        },
        "sun_azimuth_frequency": {
            "histogram": {"field": "properties.view:sun_azimuth", "interval": 5}
        },
        "off_nadir_frequency": {
            "histogram": {"field": "properties.view:off_nadir", "interval": 5}
        },
        "centroid_geohash_grid_frequency": {
            "geohash_grid": {
                "field": "properties.proj:centroid",
                "precision": 1,
            }
        },
        "centroid_geohex_grid_frequency": {
            "geohex_grid": {
                "field": "properties.proj:centroid",
                "precision": 0,
            }
        },
        "centroid_geotile_grid_frequency": {
            "geotile_grid": {
                "field": "properties.proj:centroid",
                "precision": 0,
            }
        },
        "geometry_geohash_grid_frequency": {
            "geohash_grid": {
                "field": "geometry",
                "precision": 1,
            }
        },
        "geometry_geotile_grid_frequency": {
            "geotile_grid": {
                "field": "geometry",
                "precision": 0,
            }
        },
    }

    """Utils"""

    def hash_cat_path(self, cat_path: str, collection_id: str = None) -> str:
        print(f"Hashing cat path {cat_path}")
        if cat_path == "root":
            return ""
        cat_path = cat_path.replace("catalogs/", "")
        if cat_path.endswith("/"):
            cat_path = cat_path[:-1]
        cat_path = cat_path.replace("/", ",")
        if collection_id:
            cat_path = f"{cat_path}|{collection_id}"
        print(f"Hashed cat path {cat_path}")
        return cat_path

    def unhash_cat_path(self, hashed_cat_path: str) -> Tuple[str, str]:
        print(f"Unhashing cat path {hashed_cat_path}")
        cat_path = hashed_cat_path.replace(",", "/catalogs/")
        print(cat_path)
        if cat_path:
            cat_path = "catalogs" + "/" + cat_path
        print(f"Unhashed cat path {cat_path}")
        return cat_path


    def hash_parent_id(self, cat_path: str) -> Tuple[str, str]:
        if cat_path.endswith("/"):
            cat_path = cat_path[:-1]
        if cat_path.count("/") > 0:
            cat_path, catalog_id = cat_path.rsplit("/", 1)
            cat_path = cat_path + "/"
        else:
            catalog_id = cat_path
            cat_path = ""

        hashed_cat_path = self.hash_cat_path(cat_path)
        if hashed_cat_path:
            hashed_parent_id = f"{hashed_cat_path}||{catalog_id}"
        else:
            hashed_parent_id = catalog_id
        return hashed_parent_id, catalog_id

    """CORE LOGIC"""

    async def get_all_collections(
        self, cat_path: str, token: Optional[str], limit: int, request: Request, workspaces: Optional[List[str]],
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """Retrieve a list of all collections from Elasticsearch, supporting pagination.

        Args:
            token (Optional[str]): The pagination token.
            limit (int): The number of results to return.

        Returns:
            A tuple of (collections, next pagination token if any).
        """
        search_after = None
        if token:
            search_after = [token]

        if cat_path:
            if cat_path.endswith("/catalogs"):
                cat_path = cat_path.rsplit("/", 1)[0]
            elif cat_path.endswith("catalogs"):
                cat_path = ""

        search = self.make_search()
        search = self.apply_access_filter(search=search, workspaces=workspaces)

        if cat_path != None:
            search = self.apply_catalogs_filter(search=search, catalog_path=cat_path)
            
        query = search.query.to_dict() if search.query else None
        print(query)

        response = await self.client.search(
            index=COLLECTIONS_INDEX,
            sort=[{"id": {"order": "asc"}}],
            search_after=search_after,
            size=limit,
            query=query,
        )

        hits = response["hits"]["hits"]
        collections = [
            self.collection_serializer.db_to_stac(
                cat_path=self.unhash_cat_path(hit["_source"]["_sfapi_internal"]["cat_path"]), collection=hit["_source"], request=request, extensions=self.extensions
            )
            for hit in hits
        ]

        next_token = None
        if len(hits) == limit:
            next_token = hits[-1]["sort"][0]

        return collections, next_token
    
    async def execute_collection_search(
        self,
        search: Search,
        limit: int,
        token: Optional[str],
        sort: Optional[Dict[str, Dict[str, str]]],
        collection_ids: Optional[List[str]],
        ignore_unavailable: bool = True,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """Retrieve a list of all collections from Elasticsearch, supporting pagination.

        Args:
            token (Optional[str]): The pagination token.
            limit (int): The number of results to return.

        Returns:
            A tuple of (collections, next pagination token if any).
        """
        search_after = None

        if token:
            search_after = json.loads(urlsafe_b64decode(token).decode())

        query = search.query.to_dict() if search.query else None

        print(query)

        index_param = ITEMS_INDEX #indices(collection_ids)

        max_result_window = MAX_LIMIT

        size_limit = min(limit + 1, max_result_window)

        search_task = asyncio.create_task(
            self.client.search(
                index=index_param,
                ignore_unavailable=ignore_unavailable,
                query=query,
                sort=sort or DEFAULT_SORT,
                search_after=search_after,
                size=size_limit,
            )
        )

        count_task = asyncio.create_task(
            self.client.count(
                index=index_param,
                ignore_unavailable=ignore_unavailable,
                body=search.to_dict(count=True),
            )
        )

        try:
            es_response = await search_task
        except exceptions.NotFoundError:
            raise NotFoundError(f"Collections '{collection_ids}' do not exist")

        hits = es_response["hits"]["hits"]
        collections = (hit["_source"] for hit in hits[:limit])

        next_token = None
        if len(hits) > limit and limit < max_result_window:
            if hits and (sort_array := hits[limit - 1].get("sort")):
                next_token = urlsafe_b64encode(json.dumps(sort_array).encode()).decode()

        matched = (
            es_response["hits"]["total"]["value"]
            if es_response["hits"]["total"]["relation"] == "eq"
            else None
        )
        if count_task.done():
            try:
                matched = count_task.result().get("count")
            except Exception as e:
                logger.error(f"Count task failed: {e}")

        return collections, matched, next_token
    
    async def get_all_sub_collections(
        self, cat_path: str, workspaces: Optional[List[str]],
    ) -> Tuple[List[str]]:
        """Retrieve a list of all collections from Elasticsearch, supporting pagination.

        Args:
            limit (int): The number of results to return.

        Returns:
            A tuple of (collections, next pagination token if any).
        """

        if cat_path:
            if cat_path.endswith("/catalogs"):
                cat_path = cat_path.rsplit("/", 1)[0]
            elif cat_path.endswith("catalogs"):
                cat_path = ""

        search = self.make_search()
        search = self.apply_access_filter(search=search, workspaces=workspaces)

        if cat_path != None:
            search = self.apply_catalogs_filter(search=search, catalog_path=cat_path)
            
        query = search.query.to_dict() if search.query else None
        print(query)

        response = await self.client.search(
            index=COLLECTIONS_INDEX,
            sort=[{"id": {"order": "asc"}}],
            size=10_000, # max allowed in response
            query=query,
        )

        hits = response["hits"]["hits"]
        collections = [(hit["_source"]["id"], hit["_source"]["title"]) for hit in hits]

        return collections
    
    async def get_all_sub_catalogs(
        self, cat_path: str, workspaces: Optional[List[str]]
    ) -> Tuple[List[str]]:
        """Retrieve a list of all catalogs from Elasticsearch, supporting pagination.

        Args:
            limit (int): The number of results to return.

        Returns:
            A tuple of (catalogs, next pagination token if any).
        """

        if cat_path:
            if cat_path.endswith("/catalogs"):
                cat_path = cat_path.rsplit("/", 1)[0]
            elif cat_path.endswith("catalogs"):
                cat_path = ""

        search = self.make_search()
        search = self.apply_access_filter(search=search, workspaces=workspaces)

        if cat_path != None:
            search = self.apply_catalogs_filter(search=search, catalog_path=cat_path)

        query = search.query.to_dict() if search.query else None
        print(query)

        response = await self.client.search(
            index=CATALOGS_INDEX,
            sort=[{"id": {"order": "asc"}}],
            size=10_000, # max allowed in response
            query=query,
        )

        hits = response["hits"]["hits"]
        catalogs = [(hit["_source"]["id"], hit["_source"]["title"]) for hit in hits]

        return catalogs
    
    async def get_all_catalogs(
        self, cat_path: Optional[str], token: Optional[str], limit: int, request: Request, workspaces: Optional[List[str]]
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """Retrieve a list of all catalogs from Elasticsearch, supporting pagination.

        Args:
            token (Optional[str]): The pagination token.
            limit (int): The number of results to return.

        Returns:
            A tuple of (catalogs, next pagination token if any).
        """
        search_after = None
        if token:
            search_after = [token]

        if cat_path:
            if cat_path.endswith("/catalogs"):
                cat_path = cat_path.rsplit("/", 1)[0]
            elif cat_path.endswith("catalogs"):
                cat_path = ""

        search = self.make_search()
        search = self.apply_access_filter(search=search, workspaces=workspaces)

        if cat_path != None: # need to include "" cat path to only return top-level catalogs
            search = self.apply_catalogs_filter(search=search, catalog_path=cat_path)

        query = search.query.to_dict() if search.query else None
        print(query)

        response = await self.client.search(
            index=CATALOGS_INDEX,
            sort=[{"id": {"order": "asc"}}],
            search_after=search_after,
            size=limit,
            query=query,
        )

        hits = response["hits"]["hits"]
        catalogs = []
        if not cat_path:
            base_cat_path = ""
        else:
            base_cat_path = cat_path + "/catalogs/"
        for hit in hits:
            child_cat_path = base_cat_path + hit["_source"]["id"]
            sub_catalogs = await self.get_all_sub_catalogs(cat_path=child_cat_path, workspaces=workspaces)
            print(sub_catalogs)
            sub_collections = await self.get_all_sub_collections(cat_path=child_cat_path, workspaces=workspaces)
            print(sub_collections)
            catalogs.append(
                self.catalog_serializer.db_to_stac(
                    cat_path=self.unhash_cat_path(hit["_source"]["_sfapi_internal"]["cat_path"]), catalog=hit["_source"], sub_catalogs=sub_catalogs, sub_collections=sub_collections, request=request, extensions=self.extensions
                )
            )

        next_token = None
        if len(hits) == limit:
            next_token = hits[-1]["sort"][0]

        return catalogs, next_token

    async def get_one_item(self, cat_path: str, collection_id: str, item_id: str, workspaces: Optional[List[str]], user_is_authenticated: bool) -> Dict:
        """Retrieve a single item from the database.

        Args:
            collection_id (str): The id of the Collection that the Item belongs to.
            item_id (str): The id of the Item.

        Returns:
            item (Dict): A dictionary containing the source data for the Item.

        Raises:
            NotFoundError: If the specified Item does not exist in the Collection.

        Notes:
            The Item is retrieved from the Elasticsearch database using the `client.get` method,
            with the index for the Collection as the target index and the combined `mk_item_id` as the document id.
        """

        hashed_cat_path = self.hash_cat_path(cat_path, collection_id)

        combi_item_path = hashed_cat_path + "||" + item_id

        print(f"Searching for item with id {combi_item_path}")

        try:
            item = await self.client.get(
                index=ITEMS_INDEX,
                id=combi_item_path,
            )
            item = item["_source"]
        except exceptions.NotFoundError:
            raise NotFoundError(
                f"Item {item_id} does not exist in Collection {collection_id}"
            )
        
        ## Check user has access
        access_control = item.get("_sfapi_internal", {})
        if not access_control.get("public", False):
            if not user_is_authenticated:
                raise UnauthenticatedError("User is not authenticated")
            for workspace in workspaces:
                if workspace == access_control("owner", ""):
                    return item_id
            raise UnauthorizedError("User is not authorized to access this item")

        return item

    @staticmethod
    def make_search():
        """Database logic to create a Search instance."""
        return Search().sort(*DEFAULT_SORT)

    @staticmethod
    def apply_ids_filter(search: Search, item_ids: List[str]):
        """Database logic to search a list of STAC item ids."""
        return search.filter("terms", id=item_ids)

    @staticmethod
    def apply_collections_filter(search: Search, collection_ids: List[str]):
        """Database logic to search a list of STAC collection ids."""
        return search.filter("terms", collection=collection_ids)
    
    def apply_catalogs_filter(self, search: Search, catalog_path: str):
        """Database logic to search STAC catalog path."""
        cat_path = self.hash_cat_path(catalog_path)
        return search.filter("term", **{"_sfapi_internal.cat_path": cat_path})
    
    @staticmethod
    def apply_access_filter(search: Search, workspaces: List[str]):
        """Database logic to search by access control."""
        # search = search.filter("term", **{"_sfapi_internal.public": True})
        # search = search.filter("term", **{"_sfapi_internal.owner": workspace})
        or_condition = Q("term", **{"_sfapi_internal.public": True})
        for workspace in workspaces:
            or_condition = or_condition | Q("term", **{"_sfapi_internal.owner": workspace})
        # or_condition = Q("term", **{"_sfapi_internal.public": True}) | Q("term", **{"_sfapi_internal.owner": workspace})

        # Apply the OR condition to the search
        search = search.query(or_condition)
        return search

    @staticmethod
    def apply_datetime_filter(search: Search, datetime_search):
        """Apply a filter to search based on datetime field.

        Args:
            search (Search): The search object to filter.
            datetime_search (dict): The datetime filter criteria.

        Returns:
            Search: The filtered search object.
        """
        if "eq" in datetime_search:
            search = search.filter(
                "term", **{"properties__datetime": datetime_search["eq"]}
            )
        else:
            search = search.filter(
                "range", properties__datetime={"lte": datetime_search["lte"]}
            )
            search = search.filter(
                "range", properties__datetime={"gte": datetime_search["gte"]}
            )
        return search

    @staticmethod
    def apply_bbox_filter(search: Search, bbox: List):
        """Filter search results based on bounding box.

        Args:
            search (Search): The search object to apply the filter to.
            bbox (List): The bounding box coordinates, represented as a list of four values [minx, miny, maxx, maxy].

        Returns:
            search (Search): The search object with the bounding box filter applied.

        Notes:
            The bounding box is transformed into a polygon using the `bbox2polygon` function and
            a geo_shape filter is added to the search object, set to intersect with the specified polygon.
        """
        return search.filter(
            Q(
                {
                    "geo_shape": {
                        "geometry": {
                            "shape": {
                                "type": "polygon",
                                "coordinates": bbox2polygon(*bbox),
                            },
                            "relation": "intersects",
                        }
                    }
                }
            )
        )

    @staticmethod
    def apply_intersects_filter(
        search: Search,
        intersects: Geometry,
    ):
        """Filter search results based on intersecting geometry.

        Args:
            search (Search): The search object to apply the filter to.
            intersects (Geometry): The intersecting geometry, represented as a GeoJSON-like object.

        Returns:
            search (Search): The search object with the intersecting geometry filter applied.

        Notes:
            A geo_shape filter is added to the search object, set to intersect with the specified geometry.
        """
        return search.filter(
            Q(
                {
                    "geo_shape": {
                        "geometry": {
                            "shape": {
                                "type": intersects.type.lower(),
                                "coordinates": intersects.coordinates,
                            },
                            "relation": "intersects",
                        }
                    }
                }
            )
        )

    @staticmethod
    def apply_stacql_filter(search: Search, op: str, field: str, value: float):
        """Filter search results based on a comparison between a field and a value.

        Args:
            search (Search): The search object to apply the filter to.
            op (str): The comparison operator to use. Can be 'eq' (equal), 'gt' (greater than), 'gte' (greater than or equal),
                'lt' (less than), or 'lte' (less than or equal).
            field (str): The field to perform the comparison on.
            value (float): The value to compare the field against.

        Returns:
            search (Search): The search object with the specified filter applied.
        """
        if op != "eq":
            key_filter = {field: {op: value}}
            search = search.filter(Q("range", **key_filter))
        else:
            search = search.filter("term", **{field: value})

        return search

    @staticmethod
    def apply_free_text_filter(search: Search, free_text_queries: Optional[List[str]]):
        """Database logic to perform query for search endpoint."""
        if free_text_queries is not None:
            free_text_query_string = '" OR properties.\\*:"'.join(free_text_queries)
            search = search.query(
                "query_string", query=f'properties.\\*:"{free_text_query_string}"'
            )

        return search

    @staticmethod
    def apply_cql2_filter(search: Search, _filter: Optional[Dict[str, Any]]):
        """
        Apply a CQL2 filter to an Elasticsearch Search object.

        This method transforms a dictionary representing a CQL2 filter into an Elasticsearch query
        and applies it to the provided Search object. If the filter is None, the original Search
        object is returned unmodified.

        Args:
            search (Search): The Elasticsearch Search object to which the filter will be applied.
            _filter (Optional[Dict[str, Any]]): The filter in dictionary form that needs to be applied
                                                to the search. The dictionary should follow the structure
                                                required by the `to_es` function which converts it
                                                to an Elasticsearch query.

        Returns:
            Search: The modified Search object with the filter applied if a filter is provided,
                    otherwise the original Search object.
        """
        if _filter is not None:
            es_query = filter.to_es(_filter)
            search = search.query(es_query)

        return search

    @staticmethod
    def populate_sort(sortby: List) -> Optional[Dict[str, Dict[str, str]]]:
        """Database logic to sort search instance."""
        if sortby:
            return {s.field: {"order": s.direction} for s in sortby}
        else:
            return None

    async def execute_search(
        self,
        search: Search,
        limit: int,
        token: Optional[str],
        sort: Optional[Dict[str, Dict[str, str]]],
        collection_ids: Optional[List[str]],
        ignore_unavailable: bool = True,
    ) -> Tuple[Iterable[Dict[str, Any]], Optional[int], Optional[str]]:
        """Execute a search query with limit and other optional parameters.

        Args:
            search (Search): The search query to be executed.
            limit (int): The maximum number of results to be returned.
            token (Optional[str]): The token used to return the next set of results.
            sort (Optional[Dict[str, Dict[str, str]]]): Specifies how the results should be sorted.
            collection_ids (Optional[List[str]]): The collection ids to search.
            ignore_unavailable (bool, optional): Whether to ignore unavailable collections. Defaults to True.

        Returns:
            Tuple[Iterable[Dict[str, Any]], Optional[int], Optional[str]]: A tuple containing:
                - An iterable of search results, where each result is a dictionary with keys and values representing the
                fields and values of each document.
                - The total number of results (if the count could be computed), or None if the count could not be
                computed.
                - The token to be used to retrieve the next set of results, or None if there are no more results.

        Raises:
            NotFoundError: If the collections specified in `collection_ids` do not exist.
        """
        search_after = None

        if token:
            search_after = json.loads(urlsafe_b64decode(token).decode())

        query = search.query.to_dict() if search.query else None

        print(query)

        index_param = ITEMS_INDEX #indices(collection_ids)

        max_result_window = MAX_LIMIT

        size_limit = min(limit + 1, max_result_window)

        search_task = asyncio.create_task(
            self.client.search(
                index=index_param,
                ignore_unavailable=ignore_unavailable,
                query=query,
                sort=sort or DEFAULT_SORT,
                search_after=search_after,
                size=size_limit,
            )
        )

        count_task = asyncio.create_task(
            self.client.count(
                index=index_param,
                ignore_unavailable=ignore_unavailable,
                body=search.to_dict(count=True),
            )
        )

        try:
            es_response = await search_task
        except exceptions.NotFoundError:
            raise NotFoundError(f"Collections '{collection_ids}' do not exist")

        hits = es_response["hits"]["hits"]
        items = (hit["_source"] for hit in hits[:limit])

        next_token = None
        if len(hits) > limit and limit < max_result_window:
            if hits and (sort_array := hits[limit - 1].get("sort")):
                next_token = urlsafe_b64encode(json.dumps(sort_array).encode()).decode()

        matched = (
            es_response["hits"]["total"]["value"]
            if es_response["hits"]["total"]["relation"] == "eq"
            else None
        )
        if count_task.done():
            try:
                matched = count_task.result().get("count")
            except Exception as e:
                logger.error(f"Count task failed: {e}")

        return items, matched, next_token

    """ AGGREGATE LOGIC """

    async def aggregate(
        self,
        cat_path: Optional[str],
        collection_ids: Optional[List[str]],
        aggregations: List[str],
        search: Search,
        centroid_geohash_grid_precision: int,
        centroid_geohex_grid_precision: int,
        centroid_geotile_grid_precision: int,
        geometry_geohash_grid_precision: int,
        geometry_geotile_grid_precision: int,
        datetime_frequency_interval: str,
        ignore_unavailable: Optional[bool] = True,
    ):
        """Return aggregations of STAC Items."""
        search_body: Dict[str, Any] = {}
        query = search.query.to_dict() if search.query else None
        if query:
            search_body["query"] = query

        logger.debug("Aggregations: %s", aggregations)

        def _fill_aggregation_parameters(name: str, agg: dict) -> dict:
            [key] = agg.keys()
            agg_precision = {
                "centroid_geohash_grid_frequency": centroid_geohash_grid_precision,
                "centroid_geohex_grid_frequency": centroid_geohex_grid_precision,
                "centroid_geotile_grid_frequency": centroid_geotile_grid_precision,
                "geometry_geohash_grid_frequency": geometry_geohash_grid_precision,
                "geometry_geotile_grid_frequency": geometry_geotile_grid_precision,
            }
            if name in agg_precision:
                agg[key]["precision"] = agg_precision[name]

            if key == "date_histogram":
                agg[key]["calendar_interval"] = datetime_frequency_interval

            return agg

        # include all aggregations specified
        # this will ignore aggregations with the wrong names
        search_body["aggregations"] = {
            k: _fill_aggregation_parameters(k, deepcopy(v))
            for k, v in self.aggregation_mapping.items()
            if k in aggregations
        }

        index_param = indices(collection_ids)
        search_task = asyncio.create_task(
            self.client.search(
                index=index_param,
                ignore_unavailable=ignore_unavailable,
                body=search_body,
            )
        )

        try:
            db_response = await search_task
        except exceptions.NotFoundError:
            raise NotFoundError(f"Collections '{collection_ids}' do not exist")

        return db_response

    """ TRANSACTION LOGIC """

    async def check_collection_exists(self, collection_id: str):
        """Database logic to check if a collection exists."""
        if not await self.client.exists(index=COLLECTIONS_INDEX, id=collection_id):
            raise NotFoundError(f"Collection {collection_id} does not exist")

    async def prep_create_item(
        self, item: Item, base_url: str, exist_ok: bool = False
    ) -> Item:
        """
        Preps an item for insertion into the database.

        Args:
            item (Item): The item to be prepped for insertion.
            base_url (str): The base URL used to create the item's self URL.
            exist_ok (bool): Indicates whether the item can exist already.

        Returns:
            Item: The prepped item.

        Raises:
            ConflictError: If the item already exists in the database.

        """
        #await self.check_collection_exists(collection_id=item["collection"])

        if not exist_ok and await self.client.exists(
            index=index_by_collection_id(item["collection"]),
            id=mk_item_id(item["id"], item["collection"]),
        ):
            raise ConflictError(
                f"Item {item['id']} in collection {item['collection']} already exists"
            )

        return self.item_serializer.stac_to_db(item, base_url)

    def sync_prep_create_item(
        self, item: Item, base_url: str, exist_ok: bool = False
    ) -> Item:
        """
        Prepare an item for insertion into the database.

        This method performs pre-insertion preparation on the given `item`,
        such as checking if the collection the item belongs to exists,
        and optionally verifying that an item with the same ID does not already exist in the database.

        Args:
            item (Item): The item to be inserted into the database.
            base_url (str): The base URL used for constructing URLs for the item.
            exist_ok (bool): Indicates whether the item can exist already.

        Returns:
            Item: The item after preparation is done.

        Raises:
            NotFoundError: If the collection that the item belongs to does not exist in the database.
            ConflictError: If an item with the same ID already exists in the collection.
        """
        item_id = item["id"]
        collection_id = item["collection"]
        if not self.sync_client.exists(index=COLLECTIONS_INDEX, id=collection_id):
            raise NotFoundError(f"Collection {collection_id} does not exist")

        if not exist_ok and self.sync_client.exists(
            index=index_by_collection_id(collection_id),
            id=mk_item_id(item_id, collection_id),
        ):
            raise ConflictError(
                f"Item {item_id} in collection {collection_id} already exists"
            )

        return self.item_serializer.stac_to_db(item, base_url)

    async def create_item(self, cat_path: str, collection_id: str, item: Item, workspace: str, refresh: bool = False):
        """Database logic for creating one item.

        Args:
            collection_id (str): The id of the collection from the resource path
            item (Item): The item to be created.
            refresh (bool, optional): Refresh the index after performing the operation. Defaults to False.

        Raises:
            ConflictError: If the item already exists in the database.

        Returns:
            None
        """
        # todo: check if collection exists, but cache
        item_id = item["id"]
        collection_id = item["collection"]
        hashed_cat_path = self.hash_cat_path(cat_path)
        combi_collection_id = hashed_cat_path + "||" +collection_id
        combi_item_id = self.hash_cat_path(cat_path, collection_id) + "||" + item_id

        if await self.client.exists(index=ITEMS_INDEX, id=combi_item_id):
            raise ConflictError(f"Item {item_id} already exists in Collection {collection_id}")

        try:
            parent_collection = await self.client.get(index=COLLECTIONS_INDEX, id=combi_collection_id)
            parent_collection = parent_collection["_source"]
        except exceptions.NotFoundError:
            raise NotFoundError(f"Parent collection {collection_id} not found")

        # Check if parent catalog exists
        if cat_path != "root":
            access_control = parent_collection.get("_sfapi_internal", {})
            if access_control.get("owner") != workspace:
                raise UnauthorizedError("You do not have permission to create items in this collection")
        else:
            if workspace != ADMIN_WORKSPACE:
                raise UnauthorizedError("Only admin can create items at root level")

        item.setdefault("_sfapi_internal", {})
        item["_sfapi_internal"].update({"cat_path": hashed_cat_path})
        item["_sfapi_internal"].update({"owner": workspace})
        item["_sfapi_internal"].update({"public": access_control.get("public")})

        es_resp = await self.client.index(
            index=ITEMS_INDEX,
            id=combi_item_id,
            document=item,
            refresh=refresh,
        )

        print(f"Created item with id {combi_item_id}")

        if (meta := es_resp.get("meta")) and meta.get("status") == 409:
            raise ConflictError(
                f"Item {item_id} in collection {collection_id} already exists"
            )

    async def delete_item(
        self, cat_path: str, item_id: str, collection_id: str, refresh: bool = False
    ):
        """Delete a single item from the database.

        Args:
            item_id (str): The id of the Item to be deleted.
            collection_id (str): The id of the Collection that the Item belongs to.
            refresh (bool, optional): Whether to refresh the index after the deletion. Default is False.

        Raises:
            NotFoundError: If the Item does not exist in the database.
        """

        combi_item_id = self.hash_cat_path(cat_path, collection_id) + "||" + item_id

        try:
            await self.client.delete(
                index=ITEMS_INDEX,
                id=combi_item_id,
                refresh=refresh,
            )
        except exceptions.NotFoundError:
            raise NotFoundError(
                f"Item {item_id} in collection {collection_id} not found"
            )
        
    async def find_catalog(self, cat_path: str, workspaces: Optional[List[str]], user_is_authenticated: bool) -> Catalog:
        """Find and return a catalog from the database.

        Args:
            self: The instance of the object calling this function.
            cat_path (str): The path of the catalog to be found.

        Returns:
            Catalog: The found catalog, represented as a `Catalog` object.

        Raises:
            NotFoundError: If the catalog with the given `cat_path` is not found in the database.

        Notes:
            This function searches for a catalog in the database using the specified `cat_path` and returns the found
            catalog as a `Catalog` object. If the catalog is not found, a `NotFoundError` is raised.
        """

        if cat_path.count("/") > 0:
            cat_path, catalog_id = cat_path.rsplit("/", 1)
        else:
            catalog_id = cat_path
            cat_path = "root"
        cat_path = cat_path[:-9] if cat_path.endswith("/catalogs") else cat_path # remove trailing /catalogs
        hashed_cat_path = self.hash_cat_path(cat_path)
        if hashed_cat_path:
            combi_cat_path = hashed_cat_path + "||" + catalog_id
        else:
            combi_cat_path = catalog_id

        print(f"Searching for catalog with id {combi_cat_path}")

        try:
            catalog = await self.client.get(index=CATALOGS_INDEX, id=combi_cat_path)
            catalog = catalog["_source"]
        except exceptions.NotFoundError:
            raise NotFoundError(f"Catalog {catalog_id} not found")
        
        access_control = catalog.get("_sfapi_internal")
        if not access_control.get("public", False):
            if not user_is_authenticated:
                raise UnauthenticatedError("User is not authenticated")
            for workspace in workspaces:
                if workspace == access_control("owner", ""):
                    return catalog
            raise UnauthorizedError("User is not authorized to access this catalog")

        return catalog
        

    async def create_catalog(self, cat_path: str, catalog: Catalog, workspace: str, refresh: bool = False):
        """Create a single catalog in the database.

        Args:
            cat_path (Str): The path to the parent catalog.
            catalog (Catalog): The Catalog object to be created.
            workspace (Str): The workspace sending the create request
            refresh (bool, optional): Whether to refresh the index after the creation. Default is False.

        Raises:
            ConflictError: If a Catalog with the same id already exists in the database.

        Notes:
            A new index is created for the items in the Catalog using the `create_item_index` function.
        """

        catalog_id = catalog["id"]
        hashed_cat_path = self.hash_cat_path(cat_path)

        if cat_path == "root":
            combi_catalog_id = catalog_id
        else:
            catalog_id = catalog["id"]
            combi_catalog_id = hashed_cat_path + "||" + catalog_id

        if await self.client.exists(index=CATALOGS_INDEX, id=combi_catalog_id):
            raise ConflictError(f"Catalog {catalog_id} already exists")
        
        parent_catalog = None

        # Check if parent catalog exists
        if cat_path != "root":
            parent_path, parent_id = self.hash_parent_id(cat_path)
            try:
                parent_catalog = await self.client.get(index=CATALOGS_INDEX, id=parent_path)
                parent_catalog = parent_catalog["_source"]
            except exceptions.NotFoundError:
                raise NotFoundError(f"Parent catalog {parent_id} not found")
            access_control = parent_catalog.get("_sfapi_internal", {})
            if access_control.get("owner") != workspace:
                if access_control.get("owner") == ADMIN_WORKSPACE and hashed_cat_path == USER_WRITEABLE_CATALOG:
                    pass
                else:
                    raise UnauthorizedError("You do not have permission to create catalogs in this catalog")
        else:
            if workspace != ADMIN_WORKSPACE:
                raise UnauthorizedError("Only admin can create catalogs at root level")
        if workspace == ADMIN_WORKSPACE:
            is_public = True
        elif parent_catalog and access_control.get("owner") == ADMIN_WORKSPACE:
            is_public = False # if parent catalog is owned by admin, set this private
        elif parent_catalog and access_control.get("owner") == workspace:
            is_public = access_control.get("public") # if parent catalog is owned by user, set this to parent's value
        else:
            is_public = False # otherwise set private

        catalog.setdefault("_sfapi_internal", {})
        catalog["_sfapi_internal"].update({"cat_path": hashed_cat_path})
        catalog["_sfapi_internal"].update({"owner": workspace})
        catalog["_sfapi_internal"].update({"public": is_public})

        await self.client.index(
            index=CATALOGS_INDEX,
            id=combi_catalog_id,
            document=catalog,
            refresh=refresh,
        )

        print(f"Catalog ID is {combi_catalog_id}")

        #await create_collection_index(catalog_id)

    async def update_catalog(
        self, cat_path: str, catalog: Catalog, workspace: str, refresh: bool = False
    ):
        """Update a catalog from the database.

        Args:
            self: The instance of the object calling this function.
            cat_path (str): The ID of the catalog to be updated.
            catalog (Catalog): The Catalog object to be used for the update.

        Raises:
            NotFoundError: If the catalog with the given `cat_path` is not
            found in the database.

        Notes:
            This function updates the catalog in the database using the specified
            `cat_path` and with the catalog specified in the `Catalog` object.
            If the catalog is not found, a `NotFoundError` is raised.
        """

        if cat_path.count("/") > 0:
            cat_path, catalog_id = cat_path.rsplit("/", 1)
            cat_path = cat_path + "/"
        else:
            catalog_id = cat_path
            cat_path = ""
        if cat_path == "catalogs":
            combi_cat_path = catalog_id
        else:
            cat_path = cat_path[:-9] if cat_path.endswith("/catalogs") else cat_path # remove trailing /catalogs
            hashed_cat_path = self.hash_cat_path(cat_path)
            if hashed_cat_path:
                combi_cat_path = hashed_cat_path + "||" + catalog["id"]
            else:
                combi_cat_path = catalog["id"]
        
        prev_catalog = await self.find_catalog(cat_path=cat_path)
        prev_catalog = prev_catalog["_source"]

        if catalog_id != catalog["id"]:
            # This is complex as we are using the hash of the cat_path to create the id
            raise NotImplementedError(f"Given collection IDs are different: {catalog_id} != {catalog['id']}")
            
            await self.create_collection(cat_path, collection, refresh=refresh)

            new_combi_collection_id = hashed_cat_path + collection["id"]
            await self.client.reindex(
                body={
                    "dest": {"index": f"{ITEMS_INDEX_PREFIX}"},
                    "source": {"index": f"{ITEMS_INDEX_PREFIX}"},
                    "script": {
                        "lang": "painless",
                        "source": f"""ctx._id = ctx._id.replace('{collection_id}', '{collection["id"]}'); ctx._source.collection = '{collection["id"]}' ;""",
                    },
                },
                wait_for_completion=True,
                refresh=refresh,
            )

            await self.delete_collection(cat_path, collection_id)

        else:
            # Check if workspace has permissions to update
            access_control = prev_catalog.get("_sfapi_internal", {})
            if access_control.get("owner") != workspace:
                raise UnauthorizedError("You do not have permission to create catalogs in this catalog")

            catalog.setdefault("_sfapi_internal", {})
            catalog["_sfapi_internal"].update({"cat_path": hashed_cat_path})
            catalog["_sfapi_internal"].update({"owner": workspace})
            catalog["_sfapi_internal"].update({"public": access_control.get("public")})
            await self.client.index(
                index=CATALOGS_INDEX,
                id=combi_cat_path,
                document=catalog,
                refresh=refresh,
            )

    async def delete_catalog(self, cat_path: str, workspace: str, refresh: bool = False):
        """Delete a catalog from the database.

        Parameters:
            self: The instance of the object calling this function.
            cat_path (str): The path of the catalog to be deleted.
            refresh (bool): Whether to refresh the index after the deletion (default: False).

        Raises:
            NotFoundError: If the catalog with the given `cat_path` is not found in the database.

        Notes:
            This function first verifies that the catalog with the specified `cat_path` exists in the database, and then
            deletes the catalog. If `refresh` is set to True, the index is refreshed after the deletion. Additionally, this
            function also calls `delete_item_index` to delete the index for the items in the catalog.
        """

        if cat_path.count("/") > 0:
            parent_cat_path, catalog_id = cat_path.rsplit("/", 1)
        else:
            catalog_id = cat_path
            parent_cat_path = ""
        parent_cat_path = parent_cat_path[:-9] if parent_cat_path.endswith("/catalogs") else parent_cat_path # remove trailing /catalogs
        hashed_parent_cat_path = self.hash_cat_path(parent_cat_path) 
        if hashed_parent_cat_path:
            combi_cat_path = hashed_parent_cat_path + "||" + catalog_id
        else:
            combi_cat_path = catalog_id

        hashed_cat_path = self.hash_cat_path(cat_path)

        # Check if workspace has permissions to delete
        try:
            prev_catalog = await self.client.get(index=CATALOGS_INDEX, id=combi_cat_path)
            prev_catalog = prev_catalog["_source"]
        except exceptions.NotFoundError:
            raise NotFoundError(f"Catalog {catalog_id} not found")
        access_control = prev_catalog.get("_sfapi_internal", {})
        if access_control("owner") != workspace:
            raise UnauthorizedError("You do not have permission to delete catalogs in this catalog")
        print(f"Deleting combi cat path {combi_cat_path} and hashed_cat_path {hashed_cat_path}")
        await self.client.delete(
            index=CATALOGS_INDEX, id=combi_cat_path, refresh=refresh
        )
        await delete_catalogs_by_id_prefix(hashed_cat_path)
        await delete_collections_by_id_prefix(hashed_cat_path)
        await delete_items_by_id_prefix(hashed_cat_path)
        

    async def find_collection(self, cat_path: str, collection_id: str, workspaces: Optional[List[str]], user_is_authenticated: bool) -> Collection:
        """Find and return a collection from the database.

        Args:
            self: The instance of the object calling this function.
            collection_id (str): The ID of the collection to be found.

        Returns:
            Collection: The found collection, represented as a `Collection` object.

        Raises:
            NotFoundError: If the collection with the given `collection_id` is not found in the database.

        Notes:
            This function searches for a collection in the database using the specified `collection_id` and returns the found
            collection as a `Collection` object. If the collection is not found, a `NotFoundError` is raised.
        """

        hashed_cat_path = self.hash_cat_path(cat_path)
        combi_collection_id = hashed_cat_path + "||" + collection_id

        print(f"Searching for collection with id {combi_collection_id}")

        try:
            collection = await self.client.get(index=COLLECTIONS_INDEX, id=combi_collection_id)
            collection = collection["_source"]
        except exceptions.NotFoundError:
            raise NotFoundError(f"Collection {collection_id} not found")
        
        ## Check user has access
        access_control = collection.get("_sfapi_internal", {})
        if not access_control.get("public", False):
            if not user_is_authenticated:
                raise UnauthenticatedError("User is not authenticated")
            for workspace in workspaces:
                if workspace == access_control("owner", ""):
                    return collection
            raise UnauthorizedError("User is not authorized to access this collection")

        return collection
    
    async def create_collection(self, cat_path: str, collection: Collection, workspace: str, refresh: bool = False):
        """Create a single collection in the database.

        Args:
            collection (Collection): The Collection object to be created.
            refresh (bool, optional): Whether to refresh the index after the creation. Default is False.

        Raises:
            ConflictError: If a Collection with the same id already exists in the database.

        Notes:
            A new index is created for the items in the Collection using the `create_item_index` function.
        """
        hashed_cat_path = self.hash_cat_path(cat_path)
        collection_id = collection["id"]
        combi_collection_id = hashed_cat_path + "||" + collection_id

        if await self.client.exists(index=COLLECTIONS_INDEX, id=combi_collection_id):
            raise ConflictError(f"Collection {collection_id} already exists")
            
        # Check if parent catalog exists
        if cat_path != "root":
            parent_id, catalog_id = self.hash_parent_id(cat_path)
            try:
                parent_catalog = await self.client.get(index=CATALOGS_INDEX, id=parent_id)
                parent_catalog = parent_catalog["_source"]
            except exceptions.NotFoundError:
                raise NotFoundError(f"Parent catalog {catalog_id} not found")
            access_control = parent_catalog.get("_sfapi_internal", {})
            if access_control.get("owner") != workspace:
                raise UnauthorizedError("You do not have permission to create collections in this catalog")
        else:
            if workspace != ADMIN_WORKSPACE:
                raise UnauthorizedError("Only admin can create collections at root level")


        if parent_catalog:
            is_public = access_control.get("public") # if parent catalog is owned by user, set this to parent's value
        else:
            is_public = False # else set private

        collection.setdefault("_sfapi_internal", {})
        collection["_sfapi_internal"].update({"cat_path": hashed_cat_path})
        collection["_sfapi_internal"].update({"owner": workspace})
        collection["_sfapi_internal"].update({"public": is_public})

        await self.client.index(
            index=COLLECTIONS_INDEX,
            id=combi_collection_id,
            document=collection,
            refresh=refresh,
        )

        print(f"Collection ID is {combi_collection_id}")

        # await create_item_index(collection_id)

    async def update_collection(
        self, cat_path: str, collection_id: str, collection: Collection, workspace: str, refresh: bool = False
    ):
        """Update a collection from the database.

        Args:
            self: The instance of the object calling this function.
            collection_id (str): The ID of the collection to be updated.
            collection (Collection): The Collection object to be used for the update.

        Raises:
            NotFoundError: If the collection with the given `collection_id` is not
            found in the database.

        Notes:
            This function updates the collection in the database using the specified
            `collection_id` and with the collection specified in the `Collection` object.
            If the collection is not found, a `NotFoundError` is raised.
        """

        hashed_cat_path = hashed_cat_path(cat_path)
        combi_collection_id = hashed_cat_path + "||" + collection_id
        
        try:
            prev_collection = await self.client.get(index=COLLECTIONS_INDEX, id=combi_collection_id)
            prev_collection = prev_collection["_source"]
        except exceptions.NotFoundError:
            raise NotFoundError(f"Collection {collection_id} not found")

        if collection_id != collection["id"]:
            # This is complex as we are using the hash of the cat_path to create the collection_id
            raise NotImplementedError(f"Given collection IDs are different: {collection_id} != {collection['id']}")
            
            await self.create_collection(cat_path, collection, refresh=refresh)

            new_combi_collection_id = hashed_cat_path + collection["id"]
            await self.client.reindex(
                body={
                    "dest": {"index": f"{ITEMS_INDEX_PREFIX}"},
                    "source": {"index": f"{ITEMS_INDEX_PREFIX}"},
                    "script": {
                        "lang": "painless",
                        "source": f"""ctx._id = ctx._id.replace('{collection_id}', '{collection["id"]}'); ctx._source.collection = '{collection["id"]}' ;""",
                    },
                },
                wait_for_completion=True,
                refresh=refresh,
            )

            await self.delete_collection(cat_path, collection_id)

        else:
            # Check if workspace has permissions to update collection
            access_control = prev_collection.get("_sfapi_internal", {})
            if access_control.get("owner") != workspace:
                raise UnauthorizedError("You do not have permission to update collections in this catalog")

            collection.setdefault("_sfapi_internal", {})
            collection["_sfapi_internal"].update({"cat_path": hashed_cat_path})
            collection["_sfapi_internal"].update({"owner": workspace})
            collection["_sfapi_internal"].update({"public": access_control.get("public")})
            await self.client.index(
                index=COLLECTIONS_INDEX,
                id=collection["id"],
                document=collection,
                refresh=refresh,
            )

    async def delete_collection(self, cat_path: str, collection_id: str, workspace: str, refresh: bool = False):
        """Delete a collection from the database.

        Parameters:
            self: The instance of the object calling this function.
            collection_id (str): The ID of the collection to be deleted.
            refresh (bool): Whether to refresh the index after the deletion (default: False).

        Raises:
            NotFoundError: If the collection with the given `collection_id` is not found in the database.

        Notes:
            This function first verifies that the collection with the specified `collection_id` exists in the database, and then
            deletes the collection. If `refresh` is set to True, the index is refreshed after the deletion. Additionally, this
            function also calls `delete_item_index` to delete the index for the items in the collection.
        """

        hashed_cat_path = self.hash_cat_path(cat_path)
        combi_collection_id = hashed_cat_path + "||" + collection_id

        # Check if workspace has permissions to update collection
        try:
            prev_collection = await self.client.get(index=COLLECTIONS_INDEX, id=combi_collection_id)
            prev_collection = prev_collection["_source"]
        except exceptions.NotFoundError:
            raise NotFoundError(f"Parent collection {collection_id} not found")
        access_control = prev_collection.get("_sfapi_internal", {})
        if access_control.get("owner") != workspace:
            raise UnauthorizedError("You do not have permission to delete collections in this catalog")

        await self.client.delete(
            index=COLLECTIONS_INDEX, id=combi_collection_id, refresh=refresh
        )
        # await delete_item_index(combi_collection_id)

        await delete_items_by_id_prefix_and_collection(hashed_cat_path, collection_id)


    async def bulk_async(
        self, collection_id: str, processed_items: List[Item], refresh: bool = False
    ) -> None:
        """Perform a bulk insert of items into the database asynchronously.

        Args:
            self: The instance of the object calling this function.
            collection_id (str): The ID of the collection to which the items belong.
            processed_items (List[Item]): A list of `Item` objects to be inserted into the database.
            refresh (bool): Whether to refresh the index after the bulk insert (default: False).

        Notes:
            This function performs a bulk insert of `processed_items` into the database using the specified `collection_id`. The
            insert is performed asynchronously, and the event loop is used to run the operation in a separate executor. The
            `mk_actions` function is called to generate a list of actions for the bulk insert. If `refresh` is set to True, the
            index is refreshed after the bulk insert. The function does not return any value.
        """
        await helpers.async_bulk(
            self.client,
            mk_actions(collection_id, processed_items),
            refresh=refresh,
            raise_on_error=False,
        )

    def bulk_sync(
        self, collection_id: str, processed_items: List[Item], refresh: bool = False
    ) -> None:
        """Perform a bulk insert of items into the database synchronously.

        Args:
            self: The instance of the object calling this function.
            collection_id (str): The ID of the collection to which the items belong.
            processed_items (List[Item]): A list of `Item` objects to be inserted into the database.
            refresh (bool): Whether to refresh the index after the bulk insert (default: False).

        Notes:
            This function performs a bulk insert of `processed_items` into the database using the specified `collection_id`. The
            insert is performed synchronously and blocking, meaning that the function does not return until the insert has
            completed. The `mk_actions` function is called to generate a list of actions for the bulk insert. If `refresh` is set to
            True, the index is refreshed after the bulk insert. The function does not return any value.
        """
        helpers.bulk(
            self.sync_client,
            mk_actions(collection_id, processed_items),
            refresh=refresh,
            raise_on_error=False,
        )

    # DANGER
    async def delete_items(self) -> None:
        """Danger. this is only for tests."""
        await self.client.delete_by_query(
            index=ITEM_INDICES,
            body={"query": {"match_all": {}}},
            wait_for_completion=True,
        )

    # DANGER
    async def delete_collections(self) -> None:
        """Danger. this is only for tests."""
        await self.client.delete_by_query(
            index=COLLECTIONS_INDEX,
            body={"query": {"match_all": {}}},
            wait_for_completion=True,
        )
