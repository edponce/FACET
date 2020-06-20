from abc import ABC, abstractmethod
from typing import (
    List,
    Tuple,
    Union,
    NoReturn,
)


__all__ = ['BaseSimstring']


class BaseSimstring(ABC):
    """Interface for Simstring implementations."""

    @abstractmethod
    def insert(self, string: str) -> NoReturn:
        pass

    @abstractmethod
    def search(
        self,
        query_string: str,
        *,
        alpha: float = None,
        similarity = None,
        rank: bool = True,
        update_cache: bool = True,
    ) -> Union[List[Tuple[str, float]], List[str]]:
        pass
