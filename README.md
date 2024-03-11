```
usage: java -jar neo4j-batch-updater [-cbatch <batch>] [-cpath <path>]
       [-cthread <threads>] [-h] [-pw <password>] [-rpath <path>] [-save
       <results>] [-ubatch <batch>] [-upath <path>] [-uri <uri>] [-user
       <username>] [-uthread <threads>]
 -cbatch,--collect-batch <batch>        Number of records to be processed
                                        per batch in collection steps.
                                        Note that the initial collection
                                        step does not use batching.
                                        Defaults to 10000
 -cpath,--collect-path <path>           Path to where the collect*.cypher
                                        files are. Defaults to "."
                                        (current directory)
 -cthread,--collect-threads <threads>   Number of parallel threads used to
                                        process batches in collection
                                        steps. Defaults to 4
 -h,--help                              Show this help message.
 -pw,--neo4j-password <password>        Neo4j access credentials password.
                                        Defaults to P@ssw0rd
 -rpath,--results-path <path>           Path to where the results of each
                                        step are written. Defaults to "."
                                        (current directory)
 -save,--save-results <results>         Save the resulting records of each
                                        step as CSV files. Defaults to
                                        true
 -ubatch,--update-batch <batch>         Number of records to be processed
                                        per batch in update steps.
                                        Defaults to 1000
 -upath,--update-path <path>            Path to where the update*.cypher
                                        files are. Defaults to "."
                                        (current directory)
 -uri,--neo4j-uri <uri>                 URI to the Neo4j instance or
                                        cluster. Defaults to
                                        bolt://localhost:7687
 -user,--neo4j-username <username>      Neo4j access credentials user
                                        name. Defaults to neo4j
 -uthread,--update-threads <threads>    Number of parallel threads used to
                                        process batches in update steps.
                                        Defaults to 4
```