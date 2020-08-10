import facet


def generate_sources(n):
    source = 'acetate'
    return [source] * n


# Instantiate a parallel FACET configuration
# Use default number of processes (1 per logical core)
f = facet.ParallelFacet('config/umls_redis.yaml:Search')

# Match [dir, file, raw_text]
matches = f.match(generate_sources(8))
print(matches)

# Change to 2 FACET processes
f.num_procs = 2

# Distribute in chunks of 10 items per process
matches = f.match(generate_sources(17), bulk_size=10)
print(matches)
