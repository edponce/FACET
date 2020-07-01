from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    Any,
    List,
    Dict,
    Union,
)


__all__ = ['BaseFormatter']


class BaseFormatter(ABC):
    """Class supporting results formatting and writing to stream."""

    def __call__(
        self,
        data: Dict[str, List[List[Dict[str, Any]]]],
        *,
        output: str = None,
    ) -> Any:
        formatted_data = self._format(data)

        if output is None:
            return formatted_data

        with open(output, 'w') as fd:
            # NOTE: Explicit conversion to string, if format is None we
            # want to return the data as is and be able to write to file.
            fd.write(str(formatted_data))

    @abstractmethod
    def _format(self, data) -> Union[str, bytes]:
        pass
