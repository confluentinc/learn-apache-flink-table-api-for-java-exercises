package marketplace;

import org.apache.flink.table.api.*;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

@Tag("IntegrationTest")
class CustomerServiceIntegrationTest extends FlinkIntegrationTest {
    private final String customersTableName = "`flink-table-api-java`.`marketplace`.`customers-temp`";

    private final Schema customerTableSchema = Schema.newBuilder()
            .column("customer_id", DataTypes.INT().notNull())
            .column("name", DataTypes.STRING().notNull())
            .column("address", DataTypes.STRING().notNull())
            .column("postcode", DataTypes.STRING().notNull())
            .column("city", DataTypes.STRING().notNull())
            .column("email", DataTypes.STRING().notNull())
            .build();

    private final TableDescriptor customerTableDescriptor = TableDescriptor.forConnector("confluent")
            .schema(customerTableSchema)
            .option("kafka.retention.time", "1h")
            .option("scan.startup.mode", "earliest-offset")
            .distributedBy(1, "customer_id")
            .build();

    private CustomerService customerService;

    @Override
    public void setup() {
        customerService = new CustomerService(
            testKit.tableEnvironment,
            customersTableName
        );
    }

    @Test
    @Timeout(90)
    public void allCustomers_shouldReturnTheDetailsOfAllCustomers() throws Exception {

        // Create a temporary customers table.
        testKit.createTemporaryTable(customersTableName, customerTableDescriptor);

        // Generate some customers.
        List<Row> customers = Stream.generate(() -> new CustomerBuilder().build())
            .limit(5)
            .toList();

        // Push the customers into the temporary table.
        testKit.insertInto(customersTableName, customers);

        // Execute the query.
        TableResult results = customerService.allCustomers();

        // Fetch the actual results.
        List<Row> actual = testKit.streamResult(results)
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
        // Create a temporary customers table.
        testKit.createTemporaryTable(customersTableName, customerTableDescriptor);

        // Generate some customers.
        List<Row> customers = Stream.generate(() -> new CustomerBuilder().build())
            .limit(5)
            .toList();

        // Push the customers into the temporary table.
        testKit.insertInto(customersTableName, customers);

        // Execute the query.
        TableResult results = customerService.allCustomerAddresses();

        // Fetch the actual results.
        List<Row> actual = testKit.streamResult(results)
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