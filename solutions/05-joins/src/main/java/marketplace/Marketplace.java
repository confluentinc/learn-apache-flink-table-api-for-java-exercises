package marketplace;

import io.confluent.flink.plugin.ConfluentSettings;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import java.io.File;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.Arrays;

public class Marketplace {

    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = ConfluentSettings.fromResource("/cloud.properties");
        TableEnvironment env = TableEnvironment.create(settings);

        CustomerService customers = new CustomerService(
            env,
            "`examples`.`marketplace`.`customers`"
        );
        OrderService orders = new OrderService(
            env,
            "`examples`.`marketplace`.`orders`",
            "`flink-table-api-java`.`marketplace`.`order-qualified-for-free-shipping`",
            "`flink-table-api-java`.`marketplace`.`customer-orders-collected-for-period`"
        );
        ClickService clicks = new ClickService(
            env,
            "`examples`.`marketplace`.`clicks`",
            "`examples`.`marketplace`.`orders`",
            "`flink-table-api-java`.`marketplace`.`order-placed-after-click`"
        );

        env.useCatalog("examples");
        env.useDatabase("marketplace");

        Arrays.stream(env.listTables()).forEach(System.out::println);

        customers.allCustomers();
        customers.allCustomerAddresses();
        orders.ordersOver50Dollars();
        orders.pricesWithTax(BigDecimal.valueOf(1.1));

        orders.createFreeShippingTable();
        orders.streamOrdersOver50Dollars();

        orders.createOrdersForPeriodTable();
        orders.streamOrdersForPeriod(Duration.ofMinutes(1));

        clicks.createOrderPlacedAfterClickTable();
        clicks.streamOrderPlacedAfterClick(Duration.ofMinutes(5));
    }
}
