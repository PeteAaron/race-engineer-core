"""
Resolver type aliases and protocol definitions.

Resolvers are callables that accept a ResolverRequest and return a ResolverResponse.
"""

from __future__ import annotations

from typing import Awaitable, Callable, Protocol

from race_engineer_core.models.answers import ResolverRequest, ResolverResponse


class Resolver(Protocol):
    """Synchronous resolver callable."""

    def __call__(self, request: ResolverRequest) -> ResolverResponse: ...


class AsyncResolver(Protocol):
    """Asynchronous resolver callable."""

    def __call__(self, request: ResolverRequest) -> Awaitable[ResolverResponse]: ...


ResolverFn = Callable[[ResolverRequest], ResolverResponse]
AsyncResolverFn = Callable[[ResolverRequest], Awaitable[ResolverResponse]]
