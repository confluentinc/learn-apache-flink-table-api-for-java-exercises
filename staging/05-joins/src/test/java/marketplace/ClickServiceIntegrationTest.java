package marketplace;

import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;

import static org.apache.flink.table.api.Expressions.$;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("IntegrationTest")
class ClickServiceIntegrationTest extends FlinkIntegrationTest {
    private final String clicksTableName = "`flink-table-api-java`.`marketplace`.`clicks-temp`";
    private final String ordersTableName = "`flink-table-api-java`.`marketplace`.`orders-temp`";
    private final String orderPlacedAfterClickTableName = "`flink-table-api-java`.`marketplace`.`order-placed-after-click-temp`";
    private final String orderPlacedAfterClickShortTableName = "order-placed-after-click-temp";

    private final String clicksTableDefinition =
        "CREATE TABLE IF NOT EXISTS " + clicksTableName + " (\n" +
            "  `click_id` VARCHAR(2147483647) NOT NULL,\n" +
            "  `user_id` INT NOT NULL,\n" +
            "  `url` VARCHAR(2147483647) NOT NULL,\n" +
            "  `user_agent` VARCHAR(2147483647) NOT NULL,\n" +
            "  `view_time` INT NOT NULL,\n" +
            "  `event_time` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',\n" +
            "  `$rowtime` TIMESTAMP_LTZ(3) NOT NULL METADATA VIRTUAL COMMENT 'SYSTEM',\n" +
            "  WATERMARK FOR `$rowtime` AS `$rowtime`\n" +
            ") DISTRIBUTED INTO 1 BUCKETS WITH (\n" +
            "   'kafka.retention.time' = '1 h',\n" +
            "   'scan.startup.mode' = 'earliest-offset'\n" +
            ");";

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

    private final List<String> orderTableFields = Arrays.asList("order_id", "customer_id", "product_id", "price", "event_time");
    private Integer indexOfOrderField(String fieldName) {
        return orderTableFields.indexOf(fieldName);
    }
    private final List<String> clickTableFields = Arrays.asList("click_id", "user_id", "url", "user_agent", "view_time", "event_time");
    private Integer indexOfClickField(String fieldName) {
        return clickTableFields.indexOf(fieldName);
    }

    private ClickService clickService;

    @Override
    public void setup() {
        super.setup();
        clickService = new ClickService(
            env,
            clicksTableName,
            ordersTableName,
            orderPlacedAfterClickTableName
        );
    }

    @Test
    @Timeout(60)
    public void createOrderPlacedAfterClickTable_shouldCreateTheTable() {
        deleteTable(orderPlacedAfterClickTableName);
        deleteTableOnExit(orderPlacedAfterClickTableName);

        TableResult result = clickService.createOrderPlacedAfterClickTable();

        String status = result.collect().next().getFieldAs(0);
        assertEquals("Command completed successfully.", status);

        env.useCatalog("flink-table-api-java");
        env.useDatabase("marketplace");
        String[] tables = env.listTables();
        assertTrue(
            Arrays.asList(tables).contains(orderPlacedAfterClickShortTableName),
            "Could not find the table: "+orderPlacedAfterClickShortTableName
        );

        String tableDefinition = env.executeSql(
            "SHOW CREATE TABLE `"+orderPlacedAfterClickShortTableName+"`"
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
    public void streamOrderPlacedAfterClick_shouldJoinOrdersAndClicksAndEmitANewStream() throws Exception {
        // Clean up any tables left over from previously executing this test.
        deleteTable(clicksTableName);
        deleteTable(ordersTableName);
        deleteTable(orderPlacedAfterClickTableName);

        // Create the necessary tables.
        createTemporaryTable(clicksTableName, clicksTableDefinition);
        createTemporaryTable(ordersTableName, ordersTableDefinition);
        clickService.createOrderPlacedAfterClickTable().await();
        deleteTableOnExit(orderPlacedAfterClickTableName);

        // Define some constants.
        final Duration withinTimePeriod = Duration.ofMinutes(5);
        final Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        final Instant onTime = now.plusSeconds(1);
        final Instant late = now.plus(withinTimePeriod).plusSeconds(1);

        // Define some customer Ids.
        List<Integer> customerIds = Arrays.asList(1, 2, 3, 4, 5);

        // Create some clicks.
        List<Row> expectedClicks = customerIds.stream()
            .map(customer ->
                new ClickBuilder()
                    .withUserId(customer)
                    .withTimestamp(now)
                    .build()
            )
            .toList();

        // Mutable copy of the clicks.
        List<Row> onTimeClicks = new ArrayList<>(expectedClicks);

        // Add a non-matching user Id.
        onTimeClicks.add(
            new ClickBuilder()
                .withUserId(99)
                .withTimestamp(now)
                .build()
        );

        // Randomize the list.
        Collections.shuffle(onTimeClicks);

        // Create some orders.
        List<Row> expectedOrders = customerIds.stream()
            .map(customer ->
                new OrderBuilder()
                    .withCustomerId(customer)
                    .withTimestamp(onTime)
                    .build()
            )
            .toList();

        // Mutable copy of the orders
        List<Row> onTimeOrders = new ArrayList<>(expectedOrders);

        // Add a non-matching customer Id.
        onTimeOrders.add(
            new OrderBuilder()
                .withCustomerId(101)
                .withTimestamp(onTime)
                .build()
        );

        // Randomize the list.
        Collections.shuffle(onTimeOrders);

        // Create a late click.
        Row lateClick = new ClickBuilder()
            .withUserId(1)
            .withTimestamp(late)
            .build();

        // Create a late order.
        Row lateOrder = new OrderBuilder()
            .withCustomerId(5)
            .withTimestamp(late)
            .build();

        // Push data into the destination tables.
        env.fromValues(onTimeClicks).insertInto(clicksTableName).execute();
        env.fromValues(onTimeOrders).insertInto(ordersTableName).execute();

        // We push the late data separately, to ensure it actually comes after the earlier data.
        env.fromValues(lateClick).insertInto(clicksTableName).execute();
        env.fromValues(lateOrder).insertInto(ordersTableName).execute();

        // Execute the query we are testing.
        cancelOnExit(clickService.streamOrderPlacedAfterClick(withinTimePeriod));

        // Query the destination table.
        TableResult queryResult = env.from(orderPlacedAfterClickTableName)
            .select($("*"))
            .execute();

        Set<Row> actual = new HashSet<>(
            fetchRows(queryResult)
                .limit(customerIds.size())
                .toList()
        );

        // Build the expected results.
        Set<Row> expected = new HashSet<> (
            customerIds.stream().map(customer -> {
                Row clickRow = expectedClicks.stream()
                    .filter(click -> click.getFieldAs(indexOfClickField("user_id")).equals(customer))
                    .findFirst()
                    .orElseThrow();

                Row orderRow = expectedOrders.stream()
                    .filter(click -> click.getFieldAs(indexOfOrderField("customer_id")).equals(customer))
                    .findFirst()
                    .orElseThrow();

                return Row.of(
                    customer,
                    clickRow.getField(indexOfClickField("url")),
                    clickRow.getField(indexOfClickField("event_time")),
                    orderRow.getField(indexOfOrderField("product_id")),
                    orderRow.getField(indexOfOrderField("event_time"))
                );
            }).toList()
        );

        // Assert on the results.
        assertEquals(expected, actual);
    }
}