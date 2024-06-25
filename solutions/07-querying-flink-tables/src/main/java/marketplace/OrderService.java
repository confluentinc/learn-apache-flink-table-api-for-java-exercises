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
        return env.from("examples.marketplace.orders")
            .select($("*"))
            .where($("price").isGreaterOrEqual(50))
            .execute();
    }

    public TableResult pricesWithTax(BigDecimal taxAmount) {
        return env.from("examples.marketplace.orders")
            .select(
                $("order_id"),
                $("price")
                    .cast(DataTypes.DECIMAL(10, 2))
                    .as("original_price"),
                $("price")
                    .cast(DataTypes.DECIMAL(10, 2))
                    .times(taxAmount)
                    .round(2)
                    .as("price_with_tax")
            ).execute();
    }
}
