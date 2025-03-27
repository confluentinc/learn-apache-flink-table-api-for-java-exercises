package marketplace;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
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

    private final Schema clicksTableSchema = Schema.newBuilder()
            .column("click_id", DataTypes.STRING().notNull())
            .column("user_id", DataTypes.INT().notNull())
            .column("url", DataTypes.STRING().notNull())
            .column("user_agent", DataTypes.STRING().notNull())
            .column("view_time", DataTypes.INT().notNull())
            .columnByMetadata("event_time", DataTypes.TIMESTAMP_LTZ(3), "timestamp")
            .columnByMetadata("$rowtime", DataTypes.TIMESTAMP_LTZ(3).notNull(), true).withComment("SYSTEM")
            .watermark("$rowtime", "$rowtime")
            .build();

    private final TableDescriptor clicksTableDescriptor = TableDescriptor.forConnector("confluent")
            .schema(clicksTableSchema)
            .option("kafka.retention.time", "1h")
            .option("scan.startup.mode", "earliest-offset")
            .distributedBy(1, "click_id")
            .build();

    private final Schema ordersTableSchema = Schema.newBuilder()
            .column("order_id", DataTypes.STRING().notNull())
            .column("customer_id", DataTypes.INT().notNull())
            .column("product_id", DataTypes.STRING().notNull())
            .column("price", DataTypes.DOUBLE().notNull())
            .columnByMetadata("event_time", DataTypes.TIMESTAMP_LTZ(3), "timestamp")
            .columnByMetadata("$rowtime", DataTypes.TIMESTAMP_LTZ(3).notNull(), true).withComment("SYSTEM")
            .watermark("$rowtime", "$rowtime")
            .build();

    private final TableDescriptor orderTableDescriptor = TableDescriptor.forConnector("confluent")
            .schema(ordersTableSchema)
            .option("kafka.retention.time", "1h")
            .option("scan.startup.mode", "earliest-offset")
            .distributedBy(1, "order_id")
            .build();

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
            testKit.tableEnvironment,
            clicksTableName,
            ordersTableName,
            orderPlacedAfterClickTableName
        );
    }

    @Test
    @Timeout(60)
    public void createOrderPlacedAfterClickTable_shouldCreateTheTable() throws Exception {
        TableResult result = testKit.registerTemporaryTable(orderPlacedAfterClickTableName, clickService::createOrderPlacedAfterClickTable);

        String status = result.collect().next().getFieldAs(0);
        assertEquals("Command completed successfully.", status);

        assertTrue(testKit.tableExists(orderPlacedAfterClickTableName), "Could not find the table: "+orderPlacedAfterClickTableName);

        String tableDefinition = testKit.tableEnvironment.executeSql(
            "SHOW CREATE TABLE "+orderPlacedAfterClickTableName
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
        // Create the necessary tables.
        testKit.createTemporaryTable(clicksTableName, clicksTableDescriptor);
        testKit.createTemporaryTable(ordersTableName, orderTableDescriptor);

        testKit.registerTemporaryTable(orderPlacedAfterClickTableName, clickService::createOrderPlacedAfterClickTable);

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
        testKit.insertInto(clicksTableName, onTimeClicks).await();
        testKit.insertInto(ordersTableName, onTimeOrders).await();

        // We push the late data separately, to ensure it actually comes after the earlier data.
        testKit.insertInto(clicksTableName, Collections.singletonList(lateClick)).await();
        testKit.insertInto(ordersTableName, Collections.singletonList(lateOrder)).await();

        // Execute the query we are testing.
        testKit.registerTemporaryStatement(clickService.streamOrderPlacedAfterClick(withinTimePeriod));

        // Query the destination table.
        TableResult queryResult = testKit.tableEnvironment.from(orderPlacedAfterClickTableName)
            .select($("*"))
            .execute();

        Set<Row> actual = new HashSet<>(
            testKit.streamResult(queryResult)
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