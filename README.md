# Elasticsearch Index Cloner, 
##Simple tool only to tranfer/copy/clone an elasticsearch index on different cluster using the REST endpoinds

Simple Java code to clone/replicate/copy an elasticsearch index accross different clusters, using the Scan/Scroll on the source index and Bulk upload on the destination cluster.
The command are executed trought the REST endpoint (generally port 9200). The settings and mappings by type are trasfered to the destination index (unless specified the option -keepDstIndex amd the destination index already present on the destination cluster).
In case it's required an http autentication on the source or destination clusters, you can pass these as parameters.

To build the jar with dependencies run:
mvn clean compile assembly:single

That's the list of all the options:

java -jar IndexCloner-0.0.1-SNAPSHOT-jar-with-dependencies.jar -h
usage: Main
 -srcHost <arg>        source: host:port (e.g. localhost:9200)
 -srcIndex <arg>       source: index name
 -srcUser <arg>        source: user authentication
 -srcPwd <arg>         source: password authentication

 -dstHost <arg>        destination: host:port (e.g. localhost:9200)
 -dstIndex <arg>       destination: index name
 -dstUser <arg>        destination: user authentication
 -dstPwd <arg>         destination: password authentication

 -keepDstIndex <arg>   delete destination index if already existing


Example:
java -jar IndexCloner-0.0.1-SNAPSHOT-jar-with-dependencies.jar -srcHost localhost:9200 -dstHost localhost:9200 -srcIndex movies_data0 -dstIndex movies_data1
