package marketplace;

import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;
import static org.junit.jupiter.api.Assertions.*;

@Tag("IntegrationTest")
class OrderServiceIntegrationTest extends FlinkIntegrationTest {
    private final String ordersTableName = "`flink-table-api-java`.`marketplace`.`orders-temp`";
    private final String orderQualifiedForFreeShippingTableName = "`flink-table-api-java`.`marketplace`.`order-qualified-for-free-shipping-temp`";
    private final String orderQualifiedForFreeShippingShortTableName = "order-qualified-for-free-shipping-temp";

    private final String ordersTableDefinition =
        "CREATE TABLE IF NOT EXISTS " + ordersTableName + " (\n" +
            "  `order_id` VARCHAR(2147483647) NOT NULL,\n" +
            "  `customer_id` INT NOT NULL,\n" +
            "  `product_id` VARCHAR(2147483647) NOT NULL,\n" +
            "  `price` DOUBLE NOT NULL,\n" +
            "  `event_time` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',\n" +
            "  `$rowtime` TIMESTAMP_LTZ(3) NOT NULL METADATA VIRTUAL COMMENT 'SYSTEM',\n" +
            "  WATERMARK FOR `$rowtime` AS `$rowtime`\n" +
            ") DISTRIBUTED INTO 1 BUCKETS WITH (\n" +
            "   'kafka.retention.time' = '1 h',\n" +
            "   'scan.startup.mode' = 'earliest-offset'\n" +
            ");";

    private final List<String> orderTableFields = Arrays.asList("order_id", "customer_id", "product_id", "price");
    private Integer indexOf(String fieldName) {
        return orderTableFields.indexOf(fieldName);
    }

    private OrderService orderService;

    private Row toQualifiedForFreeShippingRow(Row row) {
        return Row.of(
            row.<String>getFieldAs(indexOf("order_id")),
            Row.of(
                row.<Integer>getFieldAs(indexOf("customer_id")),
                row.<String>getFieldAs(indexOf("product_id")),
                row.<Double>getFieldAs(indexOf("price"))
            )
        );
    }

    @Override
    public void setup() {
        orderService = new OrderService(
            env,
            ordersTableName,
            orderQualifiedForFreeShippingTableName
        );
    }

    @Test
    @Timeout(90)
    public void ordersOver50Dollars_shouldOnlyReturnOrdersWithAPriceOf50DollarsOrMore() {
        // Clean up any tables left over from previously executing this test.
        deleteTable(ordersTableName);

        // Create a temporary orders table.
        createTemporaryTable(ordersTableName, ordersTableDefinition);

        // Create a set of orders with fixed prices
        Double[] prices = new Double[] { 25d, 49d, 50d, 51d, 75d };

        List<Row> orders = Arrays.stream(prices).map(price ->
            new OrderBuilder().withPrice(price).build()
        ).toList();

        // Push the orders into the temporary table.
        env.fromValues(orders).insertInto(ordersTableName).execute();

        // Execute the query.
        TableResult results = retry(() -> orderService.ordersOver50Dollars());

        // Build the expected results.
        List<Row> expected = orders.stream().filter(row -> row.<Double>getFieldAs(indexOf("price")) >= 50).toList();

        // Fetch the actual results.
        List<Row> actual = fetchRows(results)
            .limit(expected.size())
            .toList();

        // Assert on the results.
        assertEquals(new HashSet<>(expected), new HashSet<>(actual));

        Set<String> expectedFields = new HashSet<>(Arrays.asList(
            "order_id", "customer_id", "product_id", "price"
        ));
        assertTrue(actual.getFirst().getFieldNames(true).containsAll(expectedFields));
    }

    @Test
    @Timeout(90)
    public void pricesWithTax_shouldReturnTheCorrectPrices() {
        // Clean up any tables left over from previously executing this test.
        deleteTable(ordersTableName);

        // Create a temporary orders table.
        createTemporaryTable(ordersTableName, ordersTableDefinition);

        BigDecimal taxAmount = BigDecimal.valueOf(1.15);

        // Everything except 1 and 10.0 will result in a floating point precision issue.
        Double[] prices = new Double[] { 1d, 65.30d, 10.0d, 95.70d, 35.25d };

        // Create the orders.
        List<Row> orders = Arrays.stream(prices).map(price ->
            new OrderBuilder().withPrice(price).build()
        ).toList();

        // Push the orders into the temporary table.
        env.fromValues(orders).insertInto(ordersTableName).execute();

        // Execute the query.
        TableResult results = retry(() -> orderService.pricesWithTax(taxAmount));

        // Fetch the actual results.
        List<Row> actual = fetchRows(results)
            .limit(orders.size())
            .toList();

        // Build the expected results.
        List<Row> expected = orders.stream().map(row -> {
            BigDecimal originalPrice = BigDecimal.valueOf(row.<Double>getFieldAs(indexOf("price")))
                .setScale(2, RoundingMode.HALF_UP);
            BigDecimal priceWithTax = originalPrice
                .multiply(taxAmount)
                .setScale(2, RoundingMode.HALF_UP);

            return Row.of(
                row.<String>getFieldAs(indexOf("order_id")),
                originalPrice,
                priceWithTax
            );
        }).toList();

        // Assert on the results.
        assertEquals(new HashSet<>(expected), new HashSet<>(actual));

        Set<String> expectedFields = new HashSet<>(Arrays.asList(
            "order_id", "original_price", "price_with_tax"
        ));
        assertTrue(actual.getFirst().getFieldNames(true).containsAll(expectedFields));
    }

    @Test
    @Timeout(60)
    public void createFreeShippingTable_shouldCreateTheTable() {
        deleteTableOnExit(orderQualifiedForFreeShippingTableName);

        TableResult result = orderService.createFreeShippingTable();

        String status = result.collect().next().getFieldAs(0);
        assertEquals("Table '"+orderQualifiedForFreeShippingShortTableName+"' created", status);

        env.useCatalog("flink-table-api-java");
        env.useDatabase("marketplace");
        String[] tables = env.listTables();
        assertTrue(
            Arrays.asList(tables).contains(orderQualifiedForFreeShippingShortTableName),
            "Could not find the table: "+orderQualifiedForFreeShippingShortTableName
        );

        String tableDefinition = env.executeSql(
            "SHOW CREATE TABLE `"+orderQualifiedForFreeShippingShortTableName+"`"
        ).collect().next().getFieldAs(0);

        assertTrue(
            tableDefinition.contains("'connector' = 'confluent',"),
            "Incorrect connector. Expected 'confluent'"
        );
        assertTrue(
            tableDefinition.contains("'scan.startup.mode' = 'earliest-offset'"),
            "Incorrect scan.startup.mode. Expected 'earliest-offset'"
        );
    }

    @Test
    @Timeout(180)
    public void streamOrdersOver50Dollars_shouldStreamRecordsToTheTable() throws Exception {
        // Clean up any tables left over from previously executing this test.
        deleteTable(ordersTableName);
        deleteTable(orderQualifiedForFreeShippingTableName);

        // Create a temporary orders table.
        createTemporaryTable(ordersTableName, ordersTableDefinition);

        // Create the destination table.
        orderService.createFreeShippingTable().await();
        deleteTableOnExit(orderQualifiedForFreeShippingTableName);

        final int detailsPosition = 1;

        // Create a list of orders with specific prices.
        Double[] prices = new Double[] { 25d, 49d, 50d, 51d, 75d };

        List<Row> orders = Arrays.stream(prices).map(price ->
            new OrderBuilder().withPrice(price).build()
        ).toList();

        // Push the orders into the temporary table.
        env.fromValues(orders).insertInto(ordersTableName).execute();

        // Initiate the stream.
        cancelOnExit(orderService.streamOrdersOver50Dollars());

        // Query the destination table.
        TableResult queryResult = retry(() ->
            env.from(orderQualifiedForFreeShippingTableName)
                .select($("*"))
                .execute()
        );

        // Obtain the actual results.
        List<Row> actual = fetchRows(queryResult)
            .limit(Arrays.stream(prices).filter(p -> p >= 50).count())
            .toList();

        // Build the expected results
        List<Row> expected = orders.stream()
            .filter(row -> row.<Double>getFieldAs(indexOf("price")) >= 50)
            .map(this::toQualifiedForFreeShippingRow)
            .toList();

        // Assert on the results.
        assertEquals(new HashSet<>(expected), new HashSet<>(actual));

        assertEquals(
            new HashSet<>(Arrays.asList("order_id", "details")),
            actual.getFirst().getFieldNames(true)
        );

        assertEquals(
            new HashSet<>(Arrays.asList("customer_id", "product_id", "price")),
            actual.getFirst().<Row>getFieldAs(detailsPosition).getFieldNames(true)
        );
    }
}