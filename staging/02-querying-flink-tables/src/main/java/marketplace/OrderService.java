package marketplace;

import org.apache.flink.table.api.*;

import java.math.BigDecimal;
import java.time.Duration;

import static org.apache.flink.table.api.Expressions.*;

public class OrderService {
    private final TableEnvironment env;
    private final String ordersTableName;

    public OrderService(
        TableEnvironment env,
        String ordersTableName
    ) {
        this.env = env;
        this.ordersTableName = ordersTableName;
    }

    public TableResult ordersOver50Dollars() {
        // TODO
        return null;
    }

    public TableResult pricesWithTax(BigDecimal taxAmount) {
        // TODO
        return null;
    }
}
