package com.neo4j;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class BatchUpdater implements AutoCloseable {

    private static Level LOG_LEVEL = Level.DEBUG;
    private static String NEO4J_URI = "bolt://localhost:7687";
    private static String NEO4J_DATABASE = "neo4j";
    private static String NEO4J_USER = "neo4j";
    private static String NEO4J_PASSWORD = "P@ssw0rd";
    private static int COLLECT_BATCH_SIZE = 10000;
    private static int COLLECT_THREAD_COUNT = 4;
    private static String COLLECT_FILES_PATH = ".";
    private static int UPDATE_BATCH_SIZE = 1000;
    private static int UPDATE_THREAD_COUNT = 4;
    private static String UPDATE_FILES_PATH = ".";
    private static String RESULT_FILES_PATH = ".";
    private static boolean SAVE_RESULTS = true;

    private static final Logger logger = LogManager.getLogger(BatchUpdater.class);
    private final Driver driver;
    
    public BatchUpdater(String uri, String user, String password) {
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
        logger.info("Verifying connectivity to Neo4j at {}", uri);
        driver.verifyConnectivity();
    }

    @Override
    public void close() throws RuntimeException {
        driver.close();
    }

    void writeResultsFile(List<org.neo4j.driver.Record> records, String fileName) {
        try (CSVPrinter csvPrinter = new CSVPrinter(new FileWriter(Paths.get(RESULT_FILES_PATH, fileName).toFile()), CSVFormat.DEFAULT)) {
            csvPrinter.printRecord(records.get(0).keys());
            records.forEach(record -> {
                try {
                    csvPrinter.printRecord(record.values().stream().map(value -> value.toString().replace("\"", "")));
                }
                catch (IOException ioException) {
                    logger.error(ioException.getMessage());
                }
            });
        }
        catch (IOException ioException) {
            logger.error(ioException.getMessage());
        }
        logger.warn("Hack to remove extra quotes when writing records to CSV. Revisit.");
        logger.info("Results written to {}", Paths.get(RESULT_FILES_PATH, fileName));
    }

    List<org.neo4j.driver.Record> processReadCypherInBatches(String cypherQuery, List<org.neo4j.driver.Record> parameterRecords, int batchSize, int numberOfWorkerThreads) {
        List<org.neo4j.driver.Record> combinedResults = new ArrayList<org.neo4j.driver.Record>();
        if(parameterRecords == null) {
            logger.info("The following read cypher will be processed in a single batch:\n{}", cypherQuery);
            try (var session = driver.session()) {
                combinedResults = session.executeRead(tx -> {
                                    var result = tx.run(cypherQuery);
                                    return result.list();
                                });
            }
        }
        else {
            List<List<org.neo4j.driver.Record>> parameterRecordBatches = ListUtils.partition(parameterRecords.stream().collect(Collectors.toList()), batchSize);
            int parameterRecordBatchesSize = parameterRecordBatches.size();
            logger.info("The following read cypher will be processed as {} parameter batch{} of {} using {} worker thread{} and then recombined:\n{}", String.format("%,d", parameterRecordBatchesSize), parameterRecordBatchesSize == 1 ? "" : "es", String.format("%,d", parameterRecordBatches.size() == 0 ? 0 : parameterRecordBatches.get(0).size()), numberOfWorkerThreads, numberOfWorkerThreads == 1 ? "" : "s", cypherQuery);
            ExecutorService executorService = Executors.newFixedThreadPool(numberOfWorkerThreads);
            List<List<org.neo4j.driver.Record>> results = parameterRecordBatches
            .stream()
            .map(parameterRecordBatch -> CompletableFuture.supplyAsync(() -> {
                int parameterRecordBatchSize = parameterRecordBatch.size();
                logger.info("Processing batch of {} parameter record{}", String.format("%,d", parameterRecordBatchSize), parameterRecordBatchSize == 1 ? "" : "s");
                List<org.neo4j.driver.Record> batchResults;
                try (var session = driver.session()) {
                    batchResults = session.executeRead(tx -> {
                                        Map<String,Object> queryParameters = new HashMap<>();
                                        queryParameters.put("batch", parameterRecordBatch.stream().map(record -> record.asMap()).toList());
                                        var result = tx.run(cypherQuery, queryParameters);
                                        return result.list();
                                    });
                }
                int batchResultsSize = batchResults.size();
                logger.info("Retrieved {} parameter record{} from batch", String.format("%,d", batchResultsSize), batchResultsSize == 1 ? "" : "s");
                return batchResults;
            }, executorService))
            .collect(Collectors.collectingAndThen(Collectors.toList(), list -> list.stream().map(CompletableFuture::join).collect(Collectors.toList())));
            executorService.shutdown();
            combinedResults = results.stream().flatMap(List::stream).collect(Collectors.toList());
        }
        return combinedResults;
    }

    List<org.neo4j.driver.Record> collect(int batchSize, int numberOfWorkerThreads) {
        String collectionCypherFilesPath = COLLECT_FILES_PATH;
        String collectionFileNamesRegex = "^collect.*\\.cypher$";
        String defaultCollectionCypher = "MATCH (n) RETURN id(n) AS id";
        List<Path> cypherFilePaths;
        List<String> collectionCypherQueries = new ArrayList<String>();
        try {
            cypherFilePaths = Files.list(Paths.get(collectionCypherFilesPath)).filter(file -> !Files.isDirectory(file)).filter(filePath -> filePath.getFileName().toString().matches(collectionFileNamesRegex)).toList();
            if(cypherFilePaths.size() == 0) throw new IOException("No custom collection cypher files found at: \"" + collectionCypherFilesPath + "\"");
            if(cypherFilePaths.size() == 1) {
                logger.info("Found 1 custom collection cypher source file: {}", cypherFilePaths.get(0).toString());
                logger.info("Parameter records will be collected in one statement then sent for processing in batches.");
            }
            else {
                logger.info("Found {} custom collection cypher source files: {}", cypherFilePaths.size(), String.join(", ", cypherFilePaths.stream().map(filePath -> filePath.toString()).toList()));
                logger.info("Files will be processed in path order.");
                logger.info("Parameter records will be initially collected in one statement from the first file, additionally filtered in batches, and then sent for processing in batches.");
            }
            collectionCypherQueries = cypherFilePaths.stream().map(collectionCypherFilePath -> {
                try{
                  return Files.readString(collectionCypherFilePath);
                }
                catch(IOException ioException) {
                    logger.error(ioException.getMessage());
                    return "";
                }
            }).filter(cypher -> !cypher.isBlank()).toList();
        }
        catch (IOException ioException) {
            logger.error(ioException.getMessage());
        }
        if(collectionCypherQueries.size()==0){
            logger.warn("No suitable custom collection cypher could be found. Defaulting to: {}", defaultCollectionCypher);
            collectionCypherQueries = new ArrayList<String>();
            collectionCypherQueries.add(defaultCollectionCypher);
        }
        logger.info("Processing collection step 1 of {}", collectionCypherQueries.size());
        List<org.neo4j.driver.Record> recordsFromPreviousStep = processReadCypherInBatches(collectionCypherQueries.get(0), null, 0, 0);
        int recordsFromPreviousStepSize = recordsFromPreviousStep.size();
        logger.info("Retrieved {} parameter record{} from initial collection step", String.format("%,d", recordsFromPreviousStepSize), recordsFromPreviousStepSize == 1 ? "" : "s");
        if(SAVE_RESULTS && recordsFromPreviousStepSize > 0) writeResultsFile(recordsFromPreviousStep, "collect_1.csv");
        for(int stepNumber = 2; stepNumber <= collectionCypherQueries.size(); stepNumber++) {
            logger.info("Processing collection step {} of {}", stepNumber, collectionCypherQueries.size());
            recordsFromPreviousStep = processReadCypherInBatches(collectionCypherQueries.get(stepNumber-1), recordsFromPreviousStep, batchSize, numberOfWorkerThreads);
            recordsFromPreviousStepSize = recordsFromPreviousStep.size();
            logger.info("Retrieved {} parameter record{} from collection step {}", String.format("%,d", recordsFromPreviousStepSize), recordsFromPreviousStepSize == 1 ? "" : "s", stepNumber);
            if(SAVE_RESULTS && recordsFromPreviousStepSize > 0) writeResultsFile(recordsFromPreviousStep, "collect_" + stepNumber + ".csv");
        }
        return recordsFromPreviousStep;
    }

    List<org.neo4j.driver.Record> processWriteCypherInBatches(String cypherQuery, List<org.neo4j.driver.Record> parameterRecords, int batchSize, int numberOfWorkerThwrites) {
        List<org.neo4j.driver.Record> combinedResults = new ArrayList<org.neo4j.driver.Record>();
        if(parameterRecords == null) {
            logger.info("The followiong write cypher will be processed in a single batch:\n{}", cypherQuery);
            try (var session = driver.session()) {
                combinedResults = session.executeWrite(tx -> {
                                    var result = tx.run(cypherQuery);
                                    return result.list();
                                });
            }
        }
        else {
            List<List<org.neo4j.driver.Record>> parameterRecordBatches = ListUtils.partition(parameterRecords.stream().collect(Collectors.toList()), batchSize);
            int parameterRecordBatchesSize = parameterRecordBatches.size();
            logger.info("The following write cypher will be processed as {} parameter batch{} of {} using {} worker thread{} and then recombined:\n{}", String.format("%,d", parameterRecordBatchesSize), parameterRecordBatchesSize == 1 ? "" : "es", String.format("%,d", parameterRecordBatches.size() == 0 ? 0 : parameterRecordBatches.get(0).size()), numberOfWorkerThwrites, numberOfWorkerThwrites == 1 ? "" : "s", cypherQuery);
            ExecutorService executorService = Executors.newFixedThreadPool(numberOfWorkerThwrites);
            List<List<org.neo4j.driver.Record>> results = parameterRecordBatches
            .stream()
            .map(parameterRecordBatch -> CompletableFuture.supplyAsync(() -> {
                int parameterRecordBatchSize = parameterRecordBatch.size();
                logger.info("Processing batch of {} parameter record{}", String.format("%,d", parameterRecordBatchSize), parameterRecordBatchSize == 1 ? "" : "s");
                List<org.neo4j.driver.Record> batchResults;
                try (var session = driver.session()) {
                    batchResults = session.executeWrite(tx -> {
                                        Map<String,Object> queryParameters = new HashMap<>();
                                        queryParameters.put("batch", parameterRecordBatch.stream().map(record -> record.asMap()).toList());
                                        var result = tx.run(cypherQuery, queryParameters);
                                        return result.list();
                                    });
                }
                int batchResultsSize = batchResults.size();
                logger.info("Retrieved {} result record{} from batch", String.format("%,d", batchResultsSize), batchResultsSize == 1 ? "" : "s");
                return batchResults;
            }, executorService))
            .collect(Collectors.collectingAndThen(Collectors.toList(), list -> list.stream().map(CompletableFuture::join).collect(Collectors.toList())));
            executorService.shutdown();
            combinedResults = results.stream().flatMap(List::stream).collect(Collectors.toList());
        }
        return combinedResults;
    }

    List<org.neo4j.driver.Record> update(List<org.neo4j.driver.Record> parameterRecords, int batchSize, int numberOfWorkerThreads) {
        String updateCypherFilesPath = UPDATE_FILES_PATH;
        String updateFileNamesRegex = "^update.*\\.cypher$";
        String defaultUpdateCypher = "MATCH (n) RETURN count(n) AS numberOfNodes";
        List<Path> cypherFilePaths;
        List<String> updateCypherQueries = new ArrayList<String>();
        try {
            cypherFilePaths = Files.list(Paths.get(updateCypherFilesPath)).filter(file -> !Files.isDirectory(file)).filter(filePath -> filePath.getFileName().toString().matches(updateFileNamesRegex)).toList();
            if(cypherFilePaths.size() == 0) throw new IOException("No custom update cypher files found at: \"" + updateCypherFilesPath + "\"");
            logger.info("Found {} custom update cypher source file{}: {}", cypherFilePaths.size(), cypherFilePaths.size() == 1 ? "" : "s",  String.join(", ", cypherFilePaths.stream().map(filePath -> filePath.toString()).toList()));
            logger.info("Files will be processed in path order.");
            logger.info("Target records will be updated in batches.");
            updateCypherQueries = cypherFilePaths.stream().map(updateCypherFilePath -> {
                try{
                  return Files.readString(updateCypherFilePath);
                }
                catch(IOException ioException) {
                    logger.error(ioException.getMessage());
                    return "";
                }
            }).filter(cypher -> !cypher.isBlank()).toList();
        }
        catch (IOException ioException) {
            logger.error(ioException.getMessage());
        }
        if(updateCypherQueries.size()==0){
            logger.warn("No suitable custom update cypher could be found. Defaulting to: {}", defaultUpdateCypher);
            updateCypherQueries = new ArrayList<String>();
            updateCypherQueries.add(defaultUpdateCypher);
        }
        List<org.neo4j.driver.Record> recordsFromPreviousStep = parameterRecords;
        int recordsFromPreviousStepSize;
        for(int stepNumber = 1; stepNumber <= updateCypherQueries.size(); stepNumber++) {
            logger.info("Processing update step {} of {}", stepNumber, updateCypherQueries.size());
            recordsFromPreviousStep = processWriteCypherInBatches(updateCypherQueries.get(stepNumber-1), recordsFromPreviousStep, batchSize, numberOfWorkerThreads);
            recordsFromPreviousStepSize = recordsFromPreviousStep.size();
            logger.info("Retrieved {} result record{} from update step {} that will be used as parameter records for any next step", String.format("%,d", recordsFromPreviousStepSize), recordsFromPreviousStepSize == 1 ? "" : "s", stepNumber);
            if(SAVE_RESULTS && recordsFromPreviousStepSize > 0) writeResultsFile(recordsFromPreviousStep, "update_" + stepNumber + ".csv");
        }
        return recordsFromPreviousStep;
    }

    public static void main(String... args) {
        Configurator.setLevel(logger, LOG_LEVEL);
        CommandLineParser commandLineParser = new DefaultParser();
        Options options = new Options();
        options.addOption("uri", "neo4j-uri", true, "URI to the Neo4j instance or cluster. Defaults to " + NEO4J_URI);
        options.addOption("db", "neo4j-database", true, "Database to run queries against. Defaults to " + NEO4J_DATABASE);
        options.addOption("user", "neo4j-username", true, "Neo4j access credentials user name. Defaults to " + NEO4J_USER);
        options.addOption("pw", "neo4j-password", true, "Neo4j access credentials password. Defaults to " + NEO4J_PASSWORD);
        options.addOption("cb", "collect-batch-size", true, "Number of records to be processed per batch in collection steps. Note that the initial collection step does not use batching. Defaults to " + COLLECT_BATCH_SIZE);
        options.addOption("ct", "collect-threads", true, "Number of parallel threads used to process batches in collection steps. Defaults to " + COLLECT_THREAD_COUNT);
        options.addOption("cp", "collect-path", true, "Path to where the collect*.cypher files are. Defaults to \"" + COLLECT_FILES_PATH + "\" (current directory)");
        options.addOption("ub", "update-batch-size", true, "Number of records to be processed per batch in update steps. Defaults to " + UPDATE_BATCH_SIZE);
        options.addOption("ut", "update-threads", true, "Number of parallel threads used to process batches in update steps. Defaults to " + UPDATE_THREAD_COUNT);
        options.addOption("up", "update-path", true, "Path to where the update*.cypher files are. Defaults to \"" + UPDATE_FILES_PATH + "\" (current directory)");
        options.addOption("rp", "results-path", true, "Path to where the results of each step are written. Defaults to \"" + RESULT_FILES_PATH + "\" (current directory)");
        options.addOption("save", "results-save", true, "Save the resulting records of each step as CSV files. Defaults to " + SAVE_RESULTS);
        options.addOption("h", "help", false, "Show this help message.");
        options.getOptions().stream().forEach(option -> {
            String[] longOptionNameParts = option.getLongOpt().split("-");
            option.setArgName(longOptionNameParts[longOptionNameParts.length-1]);
        });
        try {
            CommandLine commandLine = commandLineParser.parse(options, args);
            if(commandLine.hasOption("help")){
                HelpFormatter formatter = new HelpFormatter();
                int terminalWidth = org.jline.terminal.TerminalBuilder.terminal().getWidth();
                if(terminalWidth > 80) formatter.setWidth(terminalWidth);
                formatter.printHelp("java -jar neo4j-batch-updater", "", options, "", true);
                return;
            }
            NEO4J_URI = commandLine.getOptionValue("neo4j-uri", NEO4J_URI);
            NEO4J_DATABASE = commandLine.getOptionValue("neo4j-database", NEO4J_DATABASE);
            NEO4J_USER = commandLine.getOptionValue("neo4j-username", NEO4J_USER);
            NEO4J_PASSWORD = commandLine.getOptionValue("neo4j-password", NEO4J_PASSWORD);
            COLLECT_BATCH_SIZE = Integer.parseInt(commandLine.getOptionValue("collect-batch", Integer.toString(COLLECT_BATCH_SIZE)));
            COLLECT_THREAD_COUNT = Integer.parseInt(commandLine.getOptionValue("collect-threads", Integer.toString(COLLECT_THREAD_COUNT)));
            COLLECT_FILES_PATH = commandLine.getOptionValue("collect-path", COLLECT_FILES_PATH);
            UPDATE_BATCH_SIZE = Integer.parseInt(commandLine.getOptionValue("update-batch", Integer.toString(UPDATE_BATCH_SIZE)));
            UPDATE_THREAD_COUNT = Integer.parseInt(commandLine.getOptionValue("update-threads", Integer.toString(UPDATE_THREAD_COUNT)));
            UPDATE_FILES_PATH = commandLine.getOptionValue("update-path", UPDATE_FILES_PATH);
            RESULT_FILES_PATH = commandLine.getOptionValue("update-path", RESULT_FILES_PATH);
            SAVE_RESULTS = Boolean.parseBoolean(commandLine.getOptionValue("results-save", Boolean.toString(SAVE_RESULTS)));
        }
        catch (ParseException | IOException exception) {
            logger.error("Unabled to parse command line options due to the following error. All defaults will be used.\n{}", exception.getMessage());
        }
        logger.info("Starting Neo4j Batch Update process");
        try (var batchUpdater = new BatchUpdater(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)) {
            List<org.neo4j.driver.Record> parameterRecords = batchUpdater.collect(COLLECT_BATCH_SIZE, COLLECT_THREAD_COUNT);
            batchUpdater.update(parameterRecords, UPDATE_BATCH_SIZE, UPDATE_THREAD_COUNT);
        }
        logger.info("Completed Neo4j Batch Update process");
    }
}