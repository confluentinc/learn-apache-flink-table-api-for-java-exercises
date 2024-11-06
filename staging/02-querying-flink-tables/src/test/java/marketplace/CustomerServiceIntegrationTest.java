package marketplace;

import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

@Tag("IntegrationTest")
class CustomerServiceIntegrationTest extends FlinkIntegrationTest {
    private final String customersTableName = "`flink-table-api-java`.`marketplace`.`customers-temp`";

    private final String customersTableDefinition =
        "CREATE TABLE " + customersTableName + " (\n" +
            "  `customer_id` INT NOT NULL,\n" +
            "  `name` VARCHAR(2147483647) NOT NULL,\n" +
            "  `address` VARCHAR(2147483647) NOT NULL,\n" +
            "  `postcode` VARCHAR(2147483647) NOT NULL,\n" +
            "  `city` VARCHAR(2147483647) NOT NULL,\n" +
            "  `email` VARCHAR(2147483647) NOT NULL\n" +
            ") DISTRIBUTED INTO 1 BUCKETS WITH (\n" +
            "   'kafka.retention.time' = '1 h',\n" +
            "   'scan.startup.mode' = 'earliest-offset'\n" +
            ");";

    private CustomerService customerService;

    @Override
    public void setup() {
        customerService = new CustomerService(
            env,
            customersTableName
        );
    }

    @Test
    @Timeout(90)
    public void allCustomers_shouldReturnTheDetailsOfAllCustomers() throws Exception {
        // Clean up any tables left over from previously executing this test.
        deleteTable(customersTableName);

        // Create a temporary customers table.
        createTemporaryTable(customersTableName, customersTableDefinition);

        // Generate some customers.
        List<Row> customers = Stream.generate(() -> new CustomerBuilder().build())
            .limit(5)
            .toList();

        // Push the customers into the temporary table.
        env.fromValues(customers).insertInto(customersTableName).execute();

        // Execute the query.
        TableResult results = customerService.allCustomers();

        // Fetch the actual results.
        List<Row> actual = fetchRows(results)
            .limit(customers.size())
            .toList();

        // Assert on the results.
        assertEquals(new HashSet<>(customers), new HashSet<>(actual));

        Set<String> expectedFields = new HashSet<>(Arrays.asList(
            "customer_id","name", "address", "postcode", "city", "email"
        ));
        assertEquals(expectedFields, actual.getFirst().getFieldNames(true));
    }

    @Test
    @Timeout(90)
    public void allCustomerAddresses_shouldReturnTheAddressesOfAllCustomers() throws Exception {
        // Clean up any tables left over from previously executing this test.
        deleteTable(customersTableName);

        // Create a temporary customers table.
        createTemporaryTable(customersTableName, customersTableDefinition);

        // Generate some customers.
        List<Row> customers = Stream.generate(() -> new CustomerBuilder().build())
            .limit(5)
            .toList();

        // Push the customers into the temporary table.
        env.fromValues(customers).insertInto(customersTableName).execute();

        // Execute the query.
        TableResult results = customerService.allCustomerAddresses();

        // Fetch the actual results.
        List<Row> actual = fetchRows(results)
            .limit(customers.size())
            .toList();

        // Assert on the results.
        assertEquals(customers.size(), actual.size());

        List<Row> expected = customers.stream()
            .map(row -> Row.project(row, new int[] {0, 2, 3, 4}))
            .toList();

        assertEquals(new HashSet<>(expected), new HashSet<>(actual));

        Set<String> expectedFields = new HashSet<>(Arrays.asList(
            "customer_id", "address", "postcode", "city"
        ));
        assertEquals(expectedFields, actual.getFirst().getFieldNames(true));
    }
}