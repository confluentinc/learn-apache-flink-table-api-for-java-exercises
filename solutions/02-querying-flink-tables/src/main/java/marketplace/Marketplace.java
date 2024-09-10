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
            "`examples`.`marketplace`.`orders`"
        );

        env.useCatalog("examples");
        env.useDatabase("marketplace");

        Arrays.stream(env.listTables()).forEach(System.out::println);

        customers.allCustomers();
        customers.allCustomerAddresses();
        orders.ordersOver50Dollars();
        orders.pricesWithTax(BigDecimal.valueOf(1.1));
    }
}