package marketplace;

import org.apache.flink.table.api.*;

import java.math.BigDecimal;
import java.time.Duration;

import static org.apache.flink.table.api.Expressions.*;

public class OrderService {
    private final TableEnvironment env;
    private final String ordersTableName;
    private final String freeShippingTableName;
    private final String ordersForPeriodTableName;

    public OrderService(
        TableEnvironment env,
        String ordersTableName,
        String freeShippingTableName,
        String ordersForPeriodTableName
    ) {
        this.env = env;
        this.ordersTableName = ordersTableName;
        this.freeShippingTableName = freeShippingTableName;
        this.ordersForPeriodTableName = ordersForPeriodTableName;
    }

    public TableResult createFreeShippingTable() {
        return env.executeSql(
            "CREATE TABLE IF NOT EXISTS "+freeShippingTableName+" (\n" +
                "   `order_id` STRING NOT NULL,\n" +
                "   `details` ROW (\n" +
                "          `customer_id` INT NOT NULL,\n" +
                "          `product_id` STRING NOT NULL,\n" +
                "          `price` DOUBLE NOT NULL   \n" +
                "   ) NOT NULL\n" +
                ") DISTRIBUTED INTO 1 BUCKETS WITH (\n" +
                "   'kafka.retention.time' = '1 h',\n" +
                "   'scan.startup.mode' = 'earliest-offset'\n" +
                ");");
    }

    public TableResult createOrdersForPeriodTable() {
        return env.executeSql(
            "CREATE TABLE IF NOT EXISTS "+ordersForPeriodTableName+" (\n" +
            "   `customer_id` INT NOT NULL,\n" +
            "   `window_start` TIMESTAMP(3) NOT NULL,\n" +
            "   `window_end` TIMESTAMP(3) NOT NULL,\n" +
            "   `period_in_seconds` BIGINT NOT NULL,\n" +
            "   `product_ids` MULTISET<STRING NOT NULL> NOT NULL\n" +
            ") DISTRIBUTED INTO 1 BUCKETS WITH (\n" +
            "   'kafka.retention.time' = '1 h',\n" +
            "   'scan.startup.mode' = 'earliest-offset'\n" +
            ");"
        );
    }

    public TableResult ordersOver50Dollars() {
        return env.from(ordersTableName)
            .select($("*"))
            .where($("price").isGreaterOrEqual(50))
            .execute();
    }

    public TableResult streamOrdersOver50Dollars() {
        return env.from(ordersTableName)
            .where($("price").isGreaterOrEqual(50))
            .select(
                $("order_id"),
                row(
                    $("customer_id"),
                    $("product_id"),
                    $("price")
                ).as("details")
            )
            .insertInto(freeShippingTableName)
            .execute();
    }

    public TableResult pricesWithTax(BigDecimal taxAmount) {
        return env.from(ordersTableName)
            .select(
                $("order_id"),
                $("price")
                    .cast(DataTypes.DECIMAL(10, 2))
                    .as("original_price"),
                $("price")
                    .cast(DataTypes.DECIMAL(10, 2))
                    .times(taxAmount)
                    .round(2)
                    .as("price_with_tax")
            ).execute();
    }

    public TableResult streamOrdersForPeriod(Duration period) {
        env.useCatalog("examples");
        env.useDatabase("marketplace");

        return env.from(ordersTableName)
            .window(
                Tumble.over(lit(period.toSeconds()).seconds())
                .on($("$rowtime"))
                .as("window")
            ).groupBy(
                $("customer_id"),$("window")
            ).select(
                $("customer_id"),
                $("window").start(),
                $("window").end(),
                lit(period.toSeconds()).seconds().as("period_in_seconds"),
                $("product_id").collect().as("product_ids")
            ).insertInto(
                ordersForPeriodTableName
            )
            .execute();
    }
}
