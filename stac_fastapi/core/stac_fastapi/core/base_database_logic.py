"""Base database logic."""

import abc
from typing import Any, Dict, Iterable, List, Optional


class BaseDatabaseLogic(abc.ABC):
    """
    Abstract base class for database logic.

    This class defines the basic structure and operations for database interactions.
    Subclasses must provide implementations for these methods.
    """

    @abc.abstractmethod
    async def get_all_collections(
        self, token: Optional[str], limit: int
    ) -> Iterable[Dict[str, Any]]:
        """Retrieve a list of all collections from the database."""
        pass

    @abc.abstractmethod
    async def get_all_catalogs(
        self, token: Optional[str], limit: int
    ) -> Iterable[Dict[str, Any]]:
        """Retrieve a list of all catalogs from the database."""
        pass

    @abc.abstractmethod
    async def get_one_item(self, collection_id: str, item_id: str) -> Dict:
        """Retrieve a single item from the database."""
        pass

    @abc.abstractmethod
    async def create_item(
        self, catalog_path: str, item: Dict, refresh: bool = False
    ) -> None:
        """Create an item in the database."""
        pass

    @abc.abstractmethod
    async def delete_item(
        self, item_id: str, collection_id: str, catalog_path: str, refresh: bool = False
    ) -> None:
        """Delete an item from the database."""
        pass

    @abc.abstractmethod
    async def create_collection(
        self,
        catalog_path: str,
        collection: Dict,
        username_header: dict,
        workspace: str,
        is_public: bool,
        refresh: bool = False,
    ) -> None:
        """Create a collection in the database."""
        pass

    @abc.abstractmethod
    async def find_collection(self, catalog_path: str, collection_id: str) -> Dict:
        """Find a collection in the database."""
        pass

    @abc.abstractmethod
    async def delete_collection(
        self, catalog_path: str, collection_id: str, refresh: bool = False
    ) -> None:
        """Delete a collection from the database."""
        pass

    @abc.abstractmethod
    async def create_catalog(
        self,
        catalog: Dict,
        access_control: List[int],
        catalog_path: Optional[str],
        refresh: bool = False,
    ) -> None:
        """Create a catalog in the database."""
        pass

    @abc.abstractmethod
    async def find_catalog(self, catalog_path: str) -> Dict:
        """Find a catalog in the database."""
        pass

    @abc.abstractmethod
    async def delete_catalog(self, catalog_path: str, refresh: bool = False) -> None:
        """Delete a catalog from the database."""
        pass
