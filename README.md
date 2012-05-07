# Wonderbee

Wonderbee is a Hive storage handler for Elastic Search based in part on Infochimps' Hadoop/Pig interface, Wonderdog.

## Requirements

## Usage

### Using ElasticSearchStorageHandler for Apache Hive

Wonderbee allows you to both write to and read from hive tables backed by Elastic Search.

#### Create an ElasticSearch backed table:

Either add the jars to hive.aux.jars.path or manually execute the following in Hive:
```
ADD JAR /path_to_jars/elasticsearch-0.19.4-SNAPSHOT.jar;
ADD JAR /path_to_jars/jline-0.9.94.jar;
ADD JAR /path_to_jars/log4j-1.2.16.jar;
ADD JAR /path_to_jars/lucene-analyzers-3.6.0.jar;
ADD JAR /path_to_jars/lucene-core-3.6.0.jar;
ADD JAR /path_to_jars/lucene-highlighter-3.6.0.jar;
ADD JAR /path_to_jars/lucene-memory-3.6.0.jar;
ADD JAR /path_to_jars/lucene-queries-3.6.0.jar;
ADD JAR /path_to_jars/json-simple-1.1.jar;
ADD JAR /path_to_jars/wonderdog-1.0.jar;
```

To create a table named user backed by an index named user_index

```
CREATE EXTERNAL TABLE user(
  id BIGINT,
  name STRING
  )
STORED BY "org.wonderbee.elasticsearch.hive.ElasticSearchStorageHandler"
TBLPROPERTIES (
  "es.config"="/path_to_elastic_elasticsearch/config/elasticsearch.yml",
  "es.path.plugins"="/path_to_elastic_elasticsearch/plugins",
  "es.location"="es://user_index/user?json=false&size=10000&id=id",
  "es.hostport"="some_node_in_the_cluster:9300");
```

Here the fields that you set in Hive (eg. 'name') are used as the field names when creating json records for elasticsearch.

#### Predicate Push Down:

For Hive query predicates <, <=, >, and >=, Wonderbee will convert this into a range query to the underlying index.

TODO: Currently this will only work with a single predicate.

#### Query Parameters

There are a few query paramaters available:

* ```json``` - (STORE only) When 'true' indicates to the StoreFunc that pre-rendered json records are being indexed. Default is false.
* ```size``` - When storing, this is used as the bulk request size (the number of records to stack up before indexing to elasticsearch). When loading, this is the number of records to fetch per request. Default 1000.
* ```q``` - (LOAD only) A free text query determining which records to load. If empty, matches all documents in the index.
* ```id``` - (STORE only) The name of the field to use as a document id. If blank (or -1) the documents are assumed to have no id and are assigned one by elasticsearch.

Note that elasticsearch.yml and the plugins directory are distributed to every machine in the cluster automatically via hadoop's distributed cache mechanism.
