package marketplace;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.math.BigDecimal;

import static org.apache.flink.table.api.Expressions.$;

public class OrderService {
    protected TableEnvironment env;

    public OrderService(TableEnvironment env) {
        this.env = env;
    }

    public TableResult ordersOver50Dollars() {
        // TODO: Implement this method.
        return null;
    }

    public TableResult pricesWithTax(BigDecimal taxAmount) {
        // TODO: Implement this method.
        return null;
    }
}
