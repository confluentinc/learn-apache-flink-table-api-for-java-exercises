package marketplace;

import io.confluent.flink.plugin.ConfluentSettings;
import org.apache.flink.table.api.TableEnvironment;

import java.math.BigDecimal;
import java.util.Arrays;

public class Marketplace {

    public static void main(String[] args) throws Exception {
        ConfluentSettings.Builder settings = ConfluentSettings.newBuilder("/cloud.properties");

        TableEnvironment env = TableEnvironment.create(settings.build());
        CustomerService customers = new CustomerService(env);
        OrderService orders = new OrderService(env);

        env.useCatalog("examples");
        env.useDatabase("marketplace");

        Arrays.stream(env.listTables()).forEach(System.out::println);

        customers.allCustomers();
        customers.allCustomerAddresses();
        orders.ordersOver50Dollars();
        orders.pricesWithTax(BigDecimal.valueOf(1.1));
    }
}
