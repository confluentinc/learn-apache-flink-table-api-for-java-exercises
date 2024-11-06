package marketplace;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.*;

public class ClickService {
    private final TableEnvironment env;
    private final String clicksTableName;
    private final String ordersTableName;
    private final String orderPlacedAfterClickTableName;

    public ClickService(
        TableEnvironment env,
        String clicksTableName,
        String ordersTableName,
        String orderPlacedAfterClickTableName
    ) {
        this.env = env;
        this.clicksTableName = clicksTableName;
        this.ordersTableName = ordersTableName;
        this.orderPlacedAfterClickTableName = orderPlacedAfterClickTableName;
    }

    public TableResult createOrderPlacedAfterClickTable() {
        return env.executeSql(
            "CREATE TABLE IF NOT EXISTS "+orderPlacedAfterClickTableName+" (\n" +
                "   `customer_id` INT NOT NULL,\n" +
                "   `clicked_url` VARCHAR(2147483647) NOT NULL,\n" +
                "   `time_of_click` TIMESTAMP_LTZ(3) NOT NULL,\n" +
                "   `purchased_product` VARCHAR(2147483647) NOT NULL,\n" +
                "   `time_of_order` TIMESTAMP_LTZ(3) NOT NULL\n" +
                ") DISTRIBUTED INTO 1 BUCKETS WITH (\n" +
                "   'kafka.retention.time' = '1 h',\n" +
                "   'scan.startup.mode' = 'earliest-offset'\n" +
                ");"
        );
    }

    public TableResult streamOrderPlacedAfterClick(Duration withinTimePeriod) {
        Table clicks = env
            .from(clicksTableName)
            .select(
                $("user_id"),
                $("url"),
                $("$rowtime").as("time_of_click")
            );

        Table orders = env
            .from(ordersTableName)
            .select(
                $("customer_id"),
                $("product_id"),
                $("$rowtime").as("time_of_order")
            );

        return clicks
            .join(orders)
            .where(
                and(
                    $("user_id").isEqual($("customer_id")),
                    $("time_of_order").isGreaterOrEqual(
                        $("time_of_click")
                    ),
                    $("time_of_order").isLess(
                        $("time_of_click").plus(lit(withinTimePeriod.toSeconds()).seconds())
                    )
                )
            )
            .select(
                $("customer_id"),
                $("url").as("clicked_url"),
                $("time_of_click"),
                $("product_id").as("purchased_product"),
                $("time_of_order")
            )
            .insertInto(orderPlacedAfterClickTableName)
            .execute();
    }
}
