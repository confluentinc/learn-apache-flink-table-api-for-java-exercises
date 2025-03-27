package marketplace;

import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class TableEnvironmentTestKit {
    public final TableEnvironment tableEnvironment;

    private final List<String> temporaryTables;
    private final List<TableResult> temporaryStatements;
    private final Logger logger;

    private static class ResolvedPath {
        public final String catalog;
        public final String database;
        public final String table;
        public final String path;

        public ResolvedPath(String catalog, String database, String table) {
            this.catalog = catalog;
            this.database = database;
            this.table = table;

            this.path = String.format("`%s`.`%s`.`%s`", catalog, database, table);
        }
    }

    public TableEnvironmentTestKit(TableEnvironment tableEnvironment) {
        this.tableEnvironment = tableEnvironment;
        this.temporaryTables = new ArrayList<>();
        this.temporaryStatements = new ArrayList<>();
        this.logger = LoggerFactory.getLogger(TableEnvironmentTestKit.class);
    }

    private ResolvedPath resolvePath(String tableName) {
        String[] tablePath = tableName.split("\\.");

        String catalog = tablePath.length == 3 ? tablePath[0].replace("`", "") : tableEnvironment.getCurrentCatalog();
        String database = tablePath.length == 3 ? tablePath[1].replace("`", "") : tableEnvironment.getCurrentDatabase();
        String table = tablePath.length == 3 ? tablePath[2].replace("`", "") : tableName;

        assert catalog != null : "Catalog should not be null";
        assert database != null : "Database should not be null";

        return new ResolvedPath(catalog, database, table);
    }

    public void createTemporaryTable(String tableName, TableDescriptor descriptor) {
        String resolvedPath = resolvePath(tableName).path;

        deleteTemporaryTable(resolvedPath);

        logger.info(String.format("Creating temporary table %s", resolvedPath));

        tableEnvironment.createTable(resolvedPath, descriptor);

        while(!tableExists(tableName)) { // createTable only initiates the SQL statement. It doesn't wait for it to complete.
            logger.info(String.format("Waiting for temporary table %s", resolvedPath));
        }

        temporaryTables.add(resolvedPath);
    }

    public TableResult registerTemporaryTable(String tableName, Supplier<TableResult> temporaryTableResultSupplier) throws Exception {
        String resolvedPath = resolvePath(tableName).path;

        deleteTemporaryTable(resolvedPath);

        logger.info(String.format("Registering temporary table %s", resolvedPath));

        TableResult result = temporaryTableResultSupplier.get();
        result.await();

        temporaryTables.add(resolvedPath);

        return result;
    }

    public void deleteTemporaryTable(String tableName) {
        String resolvedPath = resolvePath(tableName).path;

        try {
            if(tableExists(tableName)) {
                logger.info(String.format("Deleting temporary table %s", resolvedPath));
                tableEnvironment.executeSql(String.format("DROP TABLE %s", resolvedPath)).await();
            }
        } catch (Exception e) {
            logger.error(String.format("Error deleting temporary table %s", resolvedPath), e);
            throw new RuntimeException(e);
        }
    }

    public void registerTemporaryStatement(TableResult result) {
        logger.info("Registering temporary statement");

        temporaryStatements.add(result);
    }

    public void cancelTemporaryStatement(TableResult result) {
        logger.info("Cancelling temporary statement");

        result.getJobClient().orElseThrow().cancel().join();
    }

    public boolean tableExists(String tableName) {
        ResolvedPath resolvedPath = resolvePath(tableName);

        TableResult tableResult = tableEnvironment.executeSql(
                String.format("SHOW TABLES FROM `%s`.`%s`;", resolvedPath.catalog, resolvedPath.database)
        );

        List<String> tables = streamResult(tableResult).map(row -> row.<String>getFieldAs(0)).toList();
        return tables.contains(resolvedPath.table);
    }

    public TableResult insertInto(String tableName, List<Row> values) {
        String resolvedPath = resolvePath(tableName).path;

        logger.info(String.format("Populating table %s", resolvedPath));

        return tableEnvironment.fromValues(values).insertInto(resolvedPath).execute();
    }

    public Stream<Row> streamResult(TableResult result) {
        Iterable<Row> iterable = result::collect;
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    public void reset() {
        temporaryTables.forEach(this::deleteTemporaryTable);
        temporaryTables.clear();

        temporaryStatements.forEach(this::cancelTemporaryStatement);
        temporaryStatements.clear();
    }
}
