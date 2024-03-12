```
usage: java -jar neo4j-batch-updater [-cb <size>] [-cp <path>] [-ct
       <threads>] [-db <database>] [-h] [-pw <password>] [-rp <path>]
       [-save <save>] [-ub <size>] [-up <path>] [-uri <uri>] [-user
       <username>] [-ut <threads>]
 -cb,--collect-batch-size <size>     Number of records to be processed per
                                     batch in collection steps. Note that
                                     the initial collection step does not
                                     use batching. Defaults to 10000
 -cp,--collect-path <path>           Path to where the collect*.cypher
                                     files are. Defaults to "." (current
                                     directory)
 -ct,--collect-threads <threads>     Number of parallel threads used to
                                     process batches in collection steps.
                                     Defaults to 4
 -db,--neo4j-database <database>     Database to run queries against.
                                     Defaults to neo4j
 -h,--help                           Show this help message.
 -pw,--neo4j-password <password>     Neo4j access credentials password.
                                     Defaults to P@ssw0rd
 -rp,--results-path <path>           Path to where the results of each
                                     step are written. Defaults to "."
                                     (current directory)
 -save,--results-save <save>         Save the resulting records of each
                                     step as CSV files. Defaults to true
 -ub,--update-batch-size <size>      Number of records to be processed per
                                     batch in update steps. Defaults to
                                     1000
 -up,--update-path <path>            Path to where the update*.cypher
                                     files are. Defaults to "." (current
                                     directory)
 -uri,--neo4j-uri <uri>              URI to the Neo4j instance or cluster.
                                     Defaults to bolt://localhost:7687
 -user,--neo4j-username <username>   Neo4j access credentials user name.
                                     Defaults to neo4j
 -ut,--update-threads <threads>      Number of parallel threads used to
                                     process batches in update steps.
                                     Defaults to 4
```