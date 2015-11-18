# Elasticsearch Index Cloner, 
###Simple tool just to tranfer/copy/clone an elasticsearch index on different cluster using the REST endpoinds

Simple Java code to clone/replicate/copy an elasticsearch index accross different clusters, using the Scan/Scroll on the source index and Bulk upload on the destination cluster.
The command are executed trought the REST endpoint (generally port 9200). 

Installation
------------
To build the jar including all the dependencies run:
```
mvn clean compile assembly:single
```

That's the list of all the options:

Usage
------
```
java -jar IndexCloner-0.0.1-SNAPSHOT-jar-with-dependencies.jar -h
```
usage: Main
*  -srcHost          source: host:port (e.g. localhost:9200)
*  -srcIndex         source: index name
*  -srcUser          source: user authentication
*  -srcPwd           source: password authentication
*  -dstHost          destination: host:port (e.g. localhost:9200)
*  -dstIndex         destination: index name
*  -dstIndexReplicas destination: number of Replicas
*  -dstIndexShards   destination: number of Shards
*  -dstUser          destination: user authentication
*  -dstPwd           destination: password authentication
*  -keepDstIndex     delete destination index if already existing


Example of usage:
-----
```
java -jar IndexCloner-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
 -srcHost localhost:9200 -srcIndex movies -srcUser user1 -srcPwd password1 \
 -dstHost SEARCH-DEV -dstIndex movies_copy -dstIndexReplicas 2 -dstIndexShards 5 -keepDstIndex false -dstUser user2 -dstPwd password2 
```
