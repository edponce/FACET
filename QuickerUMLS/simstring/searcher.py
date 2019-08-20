from collections import defaultdict


class Searcher:

    def __init__(self, db, measure):
        self.db = db
        self.measure = measure
        self.feature_extractor = db.feature_extractor
        self.lookup_strings_result = defaultdict(dict)

    def search(self, query_string, alpha):
        features = self.feature_extractor.features(query_string)
        min_feature_size = self.measure.min_feature_size(len(features), alpha)
        max_feature_size = self.measure.max_feature_size(len(features), alpha)
        results = []

        for candidate_feature_size in range(min_feature_size, max_feature_size + 1):
            tau = self.__min_overlap(len(features), candidate_feature_size, alpha)
            results.extend(self.__overlap_join(features, tau, candidate_feature_size))

        return results

    def ranked_search(self, query_string, alpha):
        results = self.search(query_string, alpha)
        features = self.feature_extractor.features(query_string)
        results_with_score = list(map(lambda x: [self.measure.similarity(features, self.feature_extractor.features(x)), x], results))
        return sorted(results_with_score, key=lambda x: (-x[0], x[1]))

    def __min_overlap(self, query_size, candidate_feature_size, alpha):
        return self.measure.minimum_common_feature_count(query_size, candidate_feature_size, alpha)

    def __overlap_join(self, features, tau, candidate_feature_size):
        query_feature_size = len(features)
        sorted_features = sorted(features, key=lambda x: len(self.__lookup_strings_by_feature_set_size_and_feature(candidate_feature_size, x)))
        candidate_string_to_matched_count = defaultdict(int)
        results = []

        for feature in sorted_features[0:query_feature_size - tau + 1]:
            for s in self.__lookup_strings_by_feature_set_size_and_feature(candidate_feature_size, feature):
                candidate_string_to_matched_count[s] += 1

        for s in candidate_string_to_matched_count.keys():
            for i in range(query_feature_size - tau + 1, query_feature_size):
                feature = sorted_features[i]
                if s in self.__lookup_strings_by_feature_set_size_and_feature(candidate_feature_size, feature):
                    candidate_string_to_matched_count[s] += 1
                if candidate_string_to_matched_count[s] >= tau:
                    results.append(s)
                    break
                remaining_feature_count = query_feature_size - i - 1
                if candidate_string_to_matched_count[s] + remaining_feature_count < tau:
                    break
        return results

    def __lookup_strings_by_feature_set_size_and_feature(self, feature_size, feature):
        if not (feature in self.lookup_strings_result[feature_size]):
            self.lookup_strings_result[feature_size][feature] = self.db.lookup_strings_by_feature_set_size_and_feature(feature_size, feature)
        return self.lookup_strings_result[feature_size][feature]



# class RedisSearcher:
#     def __init__(self,
#                  feature_extractor,
#                  host=os.environ.get('REDIS_HOST', 'localhost'),
#                  port=os.environ.get('REDIS_PORT', 6379),
#                  database=os.environ.get('REDIS_DB', 0),
#                  **kwargs):
#         self.feature_extractor = feature_extractor
#         self.db = redis.Redis(host=host, port=port, db=database)
#         self._db = self.db.pipeline()
#         self.serializer = kwargs.get('serializer', Serializer())
#
#     def add(self, string):
#         features = self.feature_extractor.features(string)
#         if self.db.exists(len(features)):
#             # NOTE: Optimization idea is to remove duplicate features.
#             # Probably this should be handled by the feature extractor.
#             # For now, let us assume that features are unique.
#             # NOTE: Optimization idea is to use a cache for hkeys to
#             # prevent hitting database as much.
#             prev_features = self.db.hkeys(len(features))
#
#             # NOTE: Decode previous features, so that we only manage strings.
#             # Also, use set for fast membership test.
#             prev_features = set(map(bytes.decode, prev_features))
#
#             for feature in features:
#                 if feature in prev_features:
#                     strings = \
#                         self.lookup_strings_by_feature_set_size_and_feature(
#                             len(features),
#                             feature
#                         )
#                     if string not in strings:
#                         strings.add(string)
#                         self._db.hmset(
#                             len(features),
#                             {feature: self.serializer.serialize(strings)}
#                         )
#                 else:
#                     self._db.hmset(
#                         len(features),
#                         {feature: self.serializer.serialize(set((string,)))}
#                     )
#         else:
#             for feature in features:
#                 self._db.hmset(
#                     len(features),
#                     {feature: self.serializer.serialize(set((string,)))}
#                 )
#         self._db.execute()
#
#     def lookup_strings_by_feature_set_size_and_feature(self, size, feature):
#         # NOTE: Redis returns a list
#         res = self.db.hmget(size, feature)[0]
#         return self.serializer.deserialize(res) if res is not None else set()
#
#     def all(self):
#         strings = set()
#         for key in self.db.keys():
#             for values in map(self.serializer.deserialize,
#                               self.db.hvals(key)):
#                 strings.update(values)
#         return strings
#
#     def clear(self):
#         self.db.flushdb()
