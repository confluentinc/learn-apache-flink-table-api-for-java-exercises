package marketplace;

import io.confluent.flink.plugin.ConfluentSettings;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class FlinkIntegrationTest {
    protected static TableEnvironmentTestKit testKit;

    protected void setup() {};
    protected void teardown() {};

    private static final Logger logger = LoggerFactory.getLogger(TableEnvironmentTestKit.class);

    @BeforeAll
    public static void beforeAll() {
        EnvironmentSettings settings = ConfluentSettings.fromResource("/cloud.properties");
        testKit = new TableEnvironmentTestKit(TableEnvironment.create(settings));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown Detected. Cleaning up resources.");

            try {
                testKit.reset();
            } catch (Exception e) {
                System.out.println("WARNING: Shutdown may not have completed successfully. Double check that resources were not left running.");
            }
        }));
    }

    @BeforeEach
    public void beforeEach() throws Exception {
        setup();
    }

    @AfterEach
    public void afterEach() {
        testKit.reset();

        teardown();
    }
}
