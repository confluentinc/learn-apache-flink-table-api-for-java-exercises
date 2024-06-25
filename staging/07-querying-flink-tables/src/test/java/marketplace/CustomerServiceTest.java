package marketplace;

import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class CustomerServiceTest extends FlinkTableAPITest {

    private CustomerService customerService;

    @Override
    public void setup() {
        customerService = new CustomerService(env);
    }

    @Test
    public void allCustomers_shouldReturnTheDetailsOfAllCustomers() {
        TableResult results = customerService.allCustomers();
        List<Row> actual = toStream(results.collect()).limit(5).toList();

        assertEquals(5, actual.size());

        actual.forEach(row -> {
            Set<String> expectedFields = new HashSet<>(Arrays.asList(
                "customer_id", "name", "address", "postcode", "city", "email"
            ));
            assertEquals(expectedFields, row.getFieldNames(true));
            assertInstanceOf(Integer.class, row.<Integer>getFieldAs("customer_id"));
            assertInstanceOf(String.class, row.<String>getFieldAs("name"));
            assertInstanceOf(String.class, row.<String>getFieldAs("address"));
            assertInstanceOf(String.class, row.<String>getFieldAs("postcode"));
            assertInstanceOf(String.class, row.<String>getFieldAs("city"));
            assertInstanceOf(String.class, row.<String>getFieldAs("email"));
        });
    }

    @Test
    public void allCustomerAddresses_shouldReturnTheAddressesOfAllCustomers() {
        TableResult results = customerService.allCustomerAddresses();
        List<Row> actual = toStream(results.collect()).limit(5).toList();

        assertEquals(5, actual.size());

        actual.forEach(row -> {
            Set<String> expectedFields = new HashSet<>(Arrays.asList(
                "customer_id", "address", "postcode", "city"
            ));
            assertEquals(expectedFields, row.getFieldNames(true));
            assertInstanceOf(Integer.class, row.<Integer>getFieldAs("customer_id"));
            assertInstanceOf(String.class, row.<String>getFieldAs("address"));
            assertInstanceOf(String.class, row.<String>getFieldAs("postcode"));
            assertInstanceOf(String.class, row.<String>getFieldAs("city"));
        });
    }
}