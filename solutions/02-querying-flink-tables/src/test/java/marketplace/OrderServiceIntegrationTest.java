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

    @Override
    public void setup() {
        orderService = new OrderService(
            env,
            ordersTableName
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
        TableResult results = orderService.ordersOver50Dollars();

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
        TableResult results = orderService.pricesWithTax(taxAmount);

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
}