TODO
====

* Fix setup.py, __init__, modules, etc.
* Use multiprocessing in method "_get_all_matches" from "quickumls.py"
* QuickUMLS uses CPMerge, see following papers

  * 2016boguszewski
  * 2010okazaki

* Code follows the open/close principle
* The input text should not contain encoded symbols as FACET assumes ASCII/UTF-8
  text. To decode HTML characters, use

    >>> import html
    >>> html.unescape('a&#32;space')


Multiple Redis Instances
------------------------

If multiple databases are required, it is recommended to run multiple Redis
instances (and use database 0 only). This is because Redis is single-threaded
and using same instance will block when using any of its databases.
To configure multiple Redis instances:
https://www.digitalocean.com/community/questions/multiple-redis-instances-on-ubuntu-16-04
