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
        // TODO
        return null;
    }

    public TableResult streamOrderPlacedAfterClick(Duration withinTimePeriod) {
        // TODO
        return null;
    }
}
