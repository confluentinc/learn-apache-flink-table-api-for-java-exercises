package marketplace;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import static org.apache.flink.table.api.Expressions.$;

public class CustomerService {
    private final TableEnvironment env;

    public CustomerService(TableEnvironment env) {
        this.env = env;
    }

    public TableResult allCustomers() {
        return env.from("examples.marketplace.customers")
            .select($("*"))
            .execute();
    }

    public TableResult allCustomerAddresses() {
        return env.from("examples.marketplace.customers")
            .select(
                $("customer_id"),
                $("address"),
                $("postcode"),
                $("city")
            ).execute();
    }
}
