package marketplace;

import io.confluent.flink.plugin.ConfluentSettings;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import java.io.File;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.Arrays;

public class Marketplace {

    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = ConfluentSettings.fromResource("/cloud.properties");
        TableEnvironment env = TableEnvironment.create(settings);

        env.useCatalog("examples");
        env.useDatabase("marketplace");

        Arrays.stream(env.listTables()).forEach(System.out::println);
    }
}
