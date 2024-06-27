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
        // TODO: Implement this method.
        return null;
    }

    public TableResult allCustomerAddresses() {
        // TODO: Implement this method.
        return null;
    }
}
