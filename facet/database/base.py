from abc import (
    ABC,
    abstractmethod,
)


__all__ = [
    'BaseDatabase',
    'BaseKVDatabase',
]


class BaseDatabase(ABC):
    """Interface with basic database commands.

    Notes:
        * Follows the framework design pattern - parent class controls
          the execution flow and subclass provides the details.

        * 'open' method is an alias to 'connect', so derived classes should
          only provide the latter one.

        * Supports context manager schemes using 'open()' or 'connect()':
          >>> with open(...) as db
          >>>   ...

        * Semantics of database connections:
            * An instance represents a single connection to a database.

            * Configuration of instance persists during disconnects, so
              reconnections are supported.

            * Configuration options can be changed during reconnection.

            * Commits, disconnects, and close operations can be invoked
              repeatedly without triggering exceptions.

            * Close operation performs a commit, then disconnects.
    """

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def open(self, *args, **kwargs):
        self.connect(*args, **kwargs)

    def close(self, *args, commit_kwargs=None, **kwargs):
        if commit_kwargs is None:
            commit_kwargs = {}
        self.commit(**commit_kwargs)
        self.disconnect(*args, **kwargs)

    def commit(self):
        pass

    @property
    @abstractmethod
    def backend(self):
        """Returns a reference to the backend connection/database object."""
        pass

    @abstractmethod
    def configuration(self):
        """Returns general configuration information as a mapping."""
        pass

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def disconnect(self):
        pass

    @abstractmethod
    def clear(self):
        pass

    @abstractmethod
    def ping(self):
        pass


class BaseKVDatabase(BaseDatabase):
    """Interface for key-value store database.

    Notes:
        * Follows the framework design pattern - parent class controls
          the execution flow and subclass provides the details.

        * API is similar to Python dictionaries, but with limited
          functionalities for performance reasons.
    """

    def __getitem__(self, key):
        return self.get(key)

    def __setitem__(self, key, value):
        self.set(key, value)

    def __delitem__(self, key):
        self.delete(key)

    def __iter__(self):
        return iter(self.keys())

    def items(self):
        # NOTE: Derived databases might have direct methods to key/values.
        return ((k, self.get(k)) for k in self.keys())

    def values(self):
        # NOTE: Derived databases might have direct methods to key/values.
        return (self.get(k) for k in self.keys())

    def update(self, data):
        if hasattr(data, 'keys'):
            for k in data.keys():
                self.set(k, data[k])
        else:
            for k, v in data:
                self.set(k, v)

    def setdefault(self, key, value=None):
        if key in self:
            value = self.get(key)
        else:
            self.set(key, value)
        return value

    def copy(
        self,
        database: 'BaseKVDatabase',
        *,
        bulk_size: int = 1000,
        nrows: int = -1,
        **kwargs,
    ):
        """Copy rows from current database to another one.

        Args:
            database (BaseKVDatabase): Database to populate with a copy of
                items in current database.

            bulk_size (int): Number of rows to process before committing data.

            nrows (int): Max number of rows to copy. If negative value,
                all rows are copied.

        Kwargs: Options forwarded to 'database.commit()'.

        Notes:
            * Checks if database objects are the same but cannot detect if
              both objects refer to the same backend database.
        """
        # Early exit to skip commit operation when self-referencing or
        # no rows are requested,
        if self is database:
            return

        if nrows < 0:
            nrows = len(self)

        # NOTE: (Idea) If databases use the same serializers, then data
        # could be copied as is. To support this, disable serializers
        # before copying data and enable them after copy completes.
        for i, (k, v) in zip(range(1, nrows + 1), self.items()):
            database.set(k, v)
            if i % bulk_size == 0:
                database.commit(**kwargs)
        database.commit(**kwargs)

    @abstractmethod
    def __len__(self):
        pass

    @abstractmethod
    def __contains__(self, key):
        pass

    @abstractmethod
    def get(self, key):
        pass

    @abstractmethod
    def set(self, key, value):
        pass

    @abstractmethod
    def keys(self):
        pass

    @abstractmethod
    def delete(self, key):
        pass
