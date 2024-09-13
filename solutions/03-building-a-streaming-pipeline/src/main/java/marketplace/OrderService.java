package marketplace;

import org.apache.flink.table.api.*;

import java.math.BigDecimal;
import java.time.Duration;

import static org.apache.flink.table.api.Expressions.*;

public class OrderService {
    private final TableEnvironment env;
    private final String ordersTableName;
    private final String freeShippingTableName;

    public OrderService(
        TableEnvironment env,
        String ordersTableName,
        String freeShippingTableName
    ) {
        this.env = env;
        this.ordersTableName = ordersTableName;
        this.freeShippingTableName = freeShippingTableName;
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
}
