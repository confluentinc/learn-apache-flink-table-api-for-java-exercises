package marketplace;

import io.confluent.flink.plugin.ConfluentSettings;

import org.apache.flink.table.api.*;

import java.util.Arrays;

public class Marketplace {

    public static void main(String[] args) throws Exception {
        ConfluentSettings.Builder settings = ConfluentSettings.newBuilder("/cloud.properties");

        TableEnvironment env = TableEnvironment.create(settings.build());

        env.useCatalog("examples");
        env.useDatabase("marketplace");

        Arrays.stream(env.listTables()).forEach(System.out::println);
    }
}
