package marketplace;

import io.confluent.flink.plugin.ConfluentSettings;
import org.apache.flink.table.api.EnvironmentSettings;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public abstract class FlinkIntegrationTest {
    private List<TableResult> jobsToCancel;
    private List<String> topicsToDelete;
    private Thread shutdownHook;
    private boolean isShuttingDown;

    protected TableEnvironment env;
    protected AdminClient adminClient;
    protected SchemaRegistryClient registryClient;

    protected Logger logger = LoggerFactory.getLogger(this.getClass());

    protected void setup() {};
    protected void teardown() {};

    @BeforeEach
    public void mainSetup() throws Exception {
        jobsToCancel = new ArrayList<>();
        topicsToDelete = new ArrayList<>();

        EnvironmentSettings settings = ConfluentSettings.fromResource("/cloud.properties");
        env = TableEnvironment.create(settings);

        Properties properties = new Properties();
        settings.getConfiguration().toMap().forEach((k,v) ->
            properties.put(k.replace("client.kafka.", ""), v)
        );
        adminClient = AdminClient.create(properties);

        Map<String, String> schemaConfig = new HashMap<>();

        schemaConfig.put("basic.auth.credentials.source", "USER_INFO");
        schemaConfig.put(
            "basic.auth.user.info",
            properties.get("client.registry.key") + ":" + properties.get("client.registry.secret"));

        registryClient = new CachedSchemaRegistryClient(
            properties.getProperty("client.registry.url"),
            100,
            schemaConfig
        );

        isShuttingDown = false;
        shutdownHook = new Thread(() -> {
            logger.info("Shutdown Detected. Cleaning up resources.");
            isShuttingDown = true;
            mainTeardown();
        });
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        setup();
    }

    @AfterEach
    public void mainTeardown() {
        teardown();

        jobsToCancel.forEach(result ->
            result.getJobClient()
                .orElseThrow()
                .cancel()
                .join()
        );

        topicsToDelete.forEach(topic ->
            deleteTopic(topic)
        );

        if(!isShuttingDown) {
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
        }
    }

    protected TableResult retry(Supplier<TableResult> supplier) {
        return retry(3, supplier);
    }

    protected TableResult retry(int tries, Supplier<TableResult> supplier) {
        try {
            return supplier.get();
        } catch (Exception e) {
            logger.error("Failed on retryable command.", e);

            if(tries > 0) {
                logger.info("Retrying");
                return retry(tries - 1, supplier);
            } else {
                logger.info("Maximum number of tries exceeded. Failing...");
                throw e;
            }
        }
    }

    protected TableResult cancelOnExit(TableResult tableResult) {
        jobsToCancel.add(tableResult);
        return tableResult;
    }

    protected Stream<Row> fetchRows(TableResult result) {
        Iterable<Row> iterable = result::collect;
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    protected String getShortTableName(String tableName) {
        String[] tablePath = tableName.split("\\.");
        return tablePath[tablePath.length - 1].replace("`","");
    }

    protected void deleteTopic(String topicName) {
        try {
            String schemaName = topicName + "-value";
            logger.info("Deleting Schema: "+schemaName);
            if(registryClient.getAllSubjects().contains(schemaName)) {
                registryClient.deleteSubject(schemaName, false);
                registryClient.deleteSubject(schemaName, true);
            }
            logger.info("Deleted Schema: "+schemaName);
        } catch (Exception e) {
            logger.error("Error Deleting Schema", e);
        }

        try {
            if(adminClient.listTopics().names().get().contains(topicName)) {
                logger.info("Deleting Topic: " + topicName);
                KafkaFuture<Void> result = adminClient.deleteTopics(List.of(topicName)).all();

                while(!result.isDone()) {
                    logger.info("Waiting for topic to be deleted: " + topicName);
                    Thread.sleep(1000);
                }

                logger.info("Topic Deleted: " + topicName);
            }
        } catch (Exception e) {
            logger.error("Error Deleting Topic", e);
        }
    }

    protected void deleteTable(String tableName) {
        String topicName = getShortTableName(tableName);
        deleteTopic(topicName);
    }

    protected void deleteTopicOnExit(String topicName) {
        topicsToDelete.add(topicName);
    }

    protected void deleteTableOnExit(String tableName) {
        String topicName = getShortTableName(tableName);
        deleteTopicOnExit(topicName);
    }

    protected void createTemporaryTable(String fullyQualifiedTableName, String tableDefinition) {
        String topicName = getShortTableName(fullyQualifiedTableName);

        logger.info("Creating temporary table: " + fullyQualifiedTableName);

        try {
            env.executeSql(tableDefinition).await();
            deleteTopicOnExit(topicName);

            logger.info("Created temporary table: " + fullyQualifiedTableName);
        } catch (Exception e) {
            logger.error("Unable to create temporary table: " + fullyQualifiedTableName, e);
        }
    }
}
