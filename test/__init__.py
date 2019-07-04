'''
Terms:
	DDB - distributed database
	document - boolean, string, numeric data or json object
	key - unique value for document
	Storage - key-value storage for documents
	Shard - API for Storage of part of DDB
	Master - API for system of Shards


Pyshard should realize next features:
1. write single document to DDB
2. retrive single document from DDB by the key 
3. remove and pop single document from DDB by the key


Master features:
1. return Shard client by the key
2. add new Shards
3. split Shards
4. remove Shard


Shard features:
1. write single document to Storage
2. retrive single document to Storage
3. remove and pop single document from Storage
4. relocate single document from remote Shard (important for Shard addition operations)


Storage features
1. write single document
2. retrive single document
3. remove and pop single document

properties:
1. limited by memory or quantity


Usage examples:

1. cat index.txt | some_script | pyshard index_name load

This have to load all data to shards network with keys from index.txt.

2. pyshard index_name cat index.txt | grep "pattern"

This have to stream data by keys from index.txt from shards network to stdout.
'''
