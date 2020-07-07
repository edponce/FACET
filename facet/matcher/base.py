from abc import ABC, abstractmethod
from typing import (
    List,
    Tuple,
    Union,
    NoReturn,
)


__all__ = ['BaseMatcher']


class BaseMatcher(ABC):
    """Interface for implementations of matching algorithms."""

    @abstractmethod
    def insert(self, string: str) -> NoReturn:
        pass

    @abstractmethod
    def search(
        self,
        query_string: str,
        **kwargs,
    ) -> Union[List[Tuple[str, float]], List[str]]:
        pass
