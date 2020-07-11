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

        * Semantics are similar to normal database connections.

        * An instance represents a single connection to a database, and
          should connect during instance initialization.

        * 'open' method is an alias to 'connect', so derived classes should
          only provide the latter one.

        * 'close' method performs a commit and then disconnects from database.
    """
    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def open(self, *args, **kwargs):
        self.connect(*args, **kwargs)

    def close(self, *args, **kwargs):
        self.commit()
        self.disconnect(*args, **kwargs)

    def execute(self, cmd, **kwargs):
        """Method to support database-specific commands."""
        pass

    def commit(self):
        pass

    def connect(self):
        pass

    def disconnect(self):
        pass

    @abstractmethod
    def get_config(self):
        pass

    @abstractmethod
    def clear(self, **kwargs):
        """Delete database table."""
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
            return self.get(key)
        else:
            self.set(key, value)
            return value

    def copy(self, db: 'BaseKVDatabase', *, bulk_size: int = 10000):
        # NOTE: Does not checks if databases are the same object because
        # this interface does not have access to its backend database handle.
        for i, (k, v) in enumerate(self.items(), start=1):
            db.set(k, v)
            if i % bulk_size == 0:
                db.commit()
        db.commit()

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
    def set(self, key, value, **kwargs):
        pass

    @abstractmethod
    def keys(self):
        pass

    @abstractmethod
    def delete(self, key):
        pass
