from abc import ABC, abstractmethod


__all__ = ['BaseSearcher', 'DatabaseSearcher']


class BaseSearcher(ABC):
    @abstractmethod
    def __init__(self, db, feature_extractor):
        pass

    @abstractmethod
    def add(self, string):
        pass

    @abstractmethod
    def lookup_strings_by_feature_set_size_and_feature(self, size, feature):
        pass


class DatabaseSearcher(BaseSearcher):
    def __init__(self,
                 db, *,
                 feature_extractor=None):
        self.db = db
        self.feature_extractor = feature_extractor

    def add(self, string):
        features = self.feature_extractor.features(string)
        if self.db.exists(str(len(features))):
            # NOTE: Optimization idea is to remove duplicate features.
            # Probably this should be handled by the feature extractor.
            # For now, let us assume that features are unique.
            prev_features = self.db.keys(str(len(features)))

            # NOTE: Decode previous features, so that we only manage strings.
            # Also, use set for fast membership test.
            prev_features = set(map(bytes.decode, prev_features))

            for feature in features:
                if feature in prev_features:
                    strings = self.lookup_strings_by_feature_set_size_and_feature(len(features), feature)
                    if string not in strings:
                        strings.add(string)
                        self.db.set(str(len(features)), {feature: strings})
                else:
                    self.db.set(str(len(features)), {feature: set((string,))})
        else:
            for feature in features:
                self.db.set(len(features), {feature: set((string,))})
        self.db.sync()

    def lookup_strings_by_feature_set_size_and_feature(self, size, feature):
        res = self.db.get(size, feature)[0]
        return res if res is not None else set()
