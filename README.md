# Dasiy
Wrap Driver to GraphDatabaseService for Neo4j

An embedded neo4j database can be accessed via some convenient APIs
provided by GraphDatabaseService. And remote databases need to be 
accessed via Driver, a totally different APIs. This project aims to
provide a bridge to connect Driver and GraphDatabaseSerivce, to make
it easy to use GraphDataService to access remote databases.
