package marketplace;

import io.confluent.flink.plugin.ConfluentSettings;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public abstract class FlinkIntegrationTest {
    private List<TableResult> jobsToCancel;
    private List<String> tablesToDelete;
    private Thread shutdownHook;
    private boolean isShuttingDown;

    protected TableEnvironment env;

    protected Logger logger = LoggerFactory.getLogger(this.getClass());

    protected void setup() {};
    protected void teardown() {};

    @BeforeEach
    public void mainSetup() throws Exception {
        jobsToCancel = new ArrayList<>();
        tablesToDelete = new ArrayList<>();

        EnvironmentSettings settings = ConfluentSettings.fromResource("/cloud.properties");
        env = TableEnvironment.create(settings);

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

        tablesToDelete.forEach(this::deleteTable);

        if(!isShuttingDown) {
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
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

    protected void deleteTable(String tableName) {
        String[] tablePath = tableName.split("\\.");

        String catalog = tablePath[0].replace("`", "");
        String database = tablePath[1].replace("`", "");
        String table = tablePath[2].replace("`", "");

        if(Arrays.asList(env.listTables(catalog, database)).contains(table)) {
            logger.info("Deleting table {}", tableName);

            try {
                env.executeSql(String.format("DROP TABLE %s", tableName)).await();
            } catch (Exception e) {
                logger.error("Unable to delete temporary table: " + tableName, e);
            }
        }
    }

    protected void deleteTableOnExit(String tableName) {
        tablesToDelete.add(tableName);
    }

    protected void createTemporaryTable(String fullyQualifiedTableName, String tableDefinition) {
        logger.info("Creating temporary table: " + fullyQualifiedTableName);

        try {
            env.executeSql(tableDefinition).await();
            deleteTableOnExit(fullyQualifiedTableName);

            logger.info("Created temporary table: " + fullyQualifiedTableName);
        } catch (Exception e) {
            logger.error("Unable to create temporary table: " + fullyQualifiedTableName, e);
        }
    }
}
