"""link helpers."""

from typing import Any, Dict, List, Optional
from urllib.parse import ParseResult, parse_qs, urlencode, urljoin, urlparse

import attr
from stac_pydantic.links import Relations
from stac_pydantic.shared import MimeTypes
from starlette.requests import Request

# Copied from pgstac links

# These can be inferred from the item/collection, so they aren't included in the database
# Instead they are dynamically generated when querying the database using the classes defined below
INFERRED_LINK_RELS = ["self", "item", "parent", "collection", "root"]


def merge_params(url: str, newparams: Dict) -> str:
    """Merge url parameters."""
    u = urlparse(url)
    params = parse_qs(u.query)
    params.update(newparams)
    param_string = urlencode(params, True)

    href = ParseResult(
        scheme=u.scheme,
        netloc=u.netloc,
        path=u.path,
        params=u.params,
        query=param_string,
        fragment=u.fragment,
    ).geturl()
    return href


@attr.s
class BaseLinks:
    """Create inferred links common to collections and items."""

    request: Request = attr.ib()

    @property
    def base_url(self):
        """Get the base url."""
        return str(self.request.base_url)

    @property
    def url(self):
        """Get the current request url."""
        return str(self.request.url)

    def resolve(self, url):
        """Resolve url to the current request url."""
        return urljoin(str(self.base_url), str(url))

    def link_self(self) -> Dict:
        """Return the self link."""
        return dict(
            rel=Relations.self.value, type=MimeTypes.json.value, href=self.base_url
        )

    def link_root(self) -> Dict:
        """Return the catalog root."""
        return dict(
            rel=Relations.root.value, type=MimeTypes.json.value, href=self.base_url
        )

    def create_links(self) -> List[Dict[str, Any]]:
        """Return all inferred links."""
        links = []
        for name in dir(self):
            if name.startswith("link_") and callable(getattr(self, name)):
                link = getattr(self, name)()
                if link is not None:
                    links.append(link)
        return links

    async def get_links(
        self, extra_links: Optional[List[Dict[str, Any]]] = None
    ) -> List[Dict[str, Any]]:
        """
        Generate all the links.

        Get the links object for a stac resource by iterating through
        available methods on this class that start with link_.
        """
        # TODO: Pass request.json() into function so this doesn't need to be coroutine
        if self.request.method == "POST":
            self.request.postbody = await self.request.json()
        # join passed in links with generated links
        # and update relative paths
        links = self.create_links()

        if extra_links:
            # For extra links passed in,
            # add links modified with a resolved href.
            # Drop any links that are dynamically
            # determined by the server (e.g. self, parent, etc.)
            # Resolving the href allows for relative paths
            # to be stored in pgstac and for the hrefs in the
            # links of response STAC objects to be resolved
            # to the request url.
            links += [
                {**link, "href": self.resolve(link["href"])}
                for link in extra_links
                if link["rel"] not in INFERRED_LINK_RELS
            ]

        return links


@attr.s
class PagingLinks(BaseLinks):
    """Create links for paging."""

    next: Optional[str] = attr.ib(kw_only=True, default=None)

    def link_next(self) -> Optional[Dict[str, Any]]:
        """Create link for next page."""
        if self.next is not None:
            # Need to generate next link by combining the current url, including base url, with the next token
            method = self.request.method
            if method == "GET":
                parsed_url = urlparse(self.url)
                netloc = parsed_url.netloc + "/"
                query_url = self.url.split(netloc)[1]
                new_url = self.resolve(query_url)
                href = merge_params(new_url, {"token": self.next})
                link = dict(
                    rel=Relations.next.value,
                    type=MimeTypes.json.value,
                    method=method,
                    href=href,
                )
                return link
            if method == "POST":
                parsed_url = urlparse(self.url)
                netloc = parsed_url.netloc + "/"
                query_url = self.url.split(netloc)[1]
                new_url = self.resolve(query_url)
                return {
                    "rel": Relations.next,
                    "type": MimeTypes.json,
                    "method": method,
                    "href": f"{new_url}",
                    "body": {**self.request.postbody, "token": self.next},
                }

        return None
