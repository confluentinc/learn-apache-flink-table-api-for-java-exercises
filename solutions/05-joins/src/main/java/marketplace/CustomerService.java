package marketplace;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import static org.apache.flink.table.api.Expressions.$;

public class CustomerService {
    private final TableEnvironment env;
    private final String customersTableName;

    public CustomerService(
        TableEnvironment env,
        String customersTableName
    ) {
        this.env = env;
        this.customersTableName = customersTableName;
    }

    public TableResult allCustomers() {
        return env.from(customersTableName)
            .select($("*"))
            .execute();
    }

    public TableResult allCustomerAddresses() {
        return env.from(customersTableName)
            .select(
                $("customer_id"),
                $("address"),
                $("postcode"),
                $("city")
            ).execute();
    }
}
