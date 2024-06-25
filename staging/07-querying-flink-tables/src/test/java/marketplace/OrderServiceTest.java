package marketplace;

import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class OrderServiceTest extends FlinkTableAPITest {

    private OrderService orderService;

    @Override
    public void setup() {
        orderService = new OrderService(env);
    }

    @Test
    public void ordersOver50Dollars_shouldOnlyReturnOrdersWithAPriceOf50DollarsOrMore() {
        TableResult results = orderService.ordersOver50Dollars();
        List<Row> actual = toStream(results.collect()).limit(20).toList();

        assertEquals(20, actual.size());

        actual.forEach(row -> {
            Set<String> expectedFields = new HashSet<>(Arrays.asList(
                "order_id", "customer_id", "product_id", "price"
            ));
            assertEquals(expectedFields, row.getFieldNames(true));
            assertInstanceOf(String.class, row.<String>getFieldAs("order_id"));
            assertInstanceOf(Integer.class, row.<Integer>getFieldAs("customer_id"));
            assertInstanceOf(String.class, row.<String>getFieldAs("product_id"));
            assertInstanceOf(Double.class, row.<Double>getFieldAs("price"));
            assertTrue(
                row.<Double>getFieldAs("price") >= 50,
                "Price must be greater than or equal to 50"
            );
        });
    }

    @Test void pricesWithTax_shouldHaveTheCorrectFieldsAndDataTypes() {
        BigDecimal taxAmount = BigDecimal.valueOf(1.15);
        TableResult results = orderService.pricesWithTax(taxAmount);
        List<Row> actual = toStream(results.collect()).limit(10).toList();

        assertEquals(10, actual.size());

        actual.forEach(row -> {
            Set<String> expectedFields = new HashSet<>(Arrays.asList(
                "order_id", "original_price", "price_with_tax"
            ));
            assertEquals(expectedFields, row.getFieldNames(true));
            assertInstanceOf(String.class, row.<String>getFieldAs("order_id"));
            assertInstanceOf(BigDecimal.class, row.<BigDecimal>getFieldAs("original_price"));
            assertInstanceOf(BigDecimal.class, row.<BigDecimal>getFieldAs("price_with_tax"));
        });
    }

    @Test
    public void pricesWithTax_shouldReturnTheCorrectPrices() {
        BigDecimal taxAmount = BigDecimal.valueOf(1.15);
        TableResult results = orderService.pricesWithTax(taxAmount);
        List<Row> actual = toStream(results.collect()).limit(1000).toList();

        actual.forEach(row -> {
            Double originalPrice = doubleValue(row.getField("original_price"));
            Double withTax = doubleValue(row.getField("price_with_tax"));
            Double expectedAmount = doubleValue(BigDecimal.valueOf(originalPrice)
                .multiply(taxAmount)
                .setScale(2, RoundingMode.HALF_UP));

            assertEquals(withTax, expectedAmount, String.format(
                "Incorrect with_tax value. Original Price: %s, With Tax %s, Expected %s.",
                originalPrice, withTax, expectedAmount
            ));
        });
    }

    // NOTE: This isn't the right way to do this.
    // Technically, the values being used should always be a BigDecimal.
    // We should use the getFieldAs method to convert to a BigDecimal.
    // However, for this course, we want a version of the code that uses Doubles instead.
    // This method is a hack to allow the test to run with either Double or BigDecimal.
    public Double doubleValue(Object value) {
        if(value instanceof Double) {
            return (Double) value;
        } else if(value instanceof BigDecimal) {
            return ((BigDecimal) value).doubleValue();
        } else {
            return fail("Invalid Type. Expected Double or BigDecimal.");
        }
    }
}