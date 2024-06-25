package marketplace;

import io.confluent.flink.plugin.ConfluentSettings;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public abstract class FlinkTableAPITest {
    protected static TableEnvironment env;
    private List<TableResult> cleanupList;
    public void setup() {};
    public void teardown() {};

    @BeforeEach
    public void mainSetup() {
        cleanupList = new ArrayList<>();
        if(env == null) {
            ConfluentSettings.Builder settings = ConfluentSettings.newBuilder("/cloud.properties");
            env = TableEnvironment.create(settings.build());
        }

        setup();
    }

    @AfterEach
    public void mainTeardown() {
        cleanupList.forEach(tableResult -> {
            JobClient client = tableResult.getJobClient().orElseThrow();
            client.cancel().thenRun(() -> System.out.println("Job Canceled During Cleanup"));
        });

        teardown();
    }

    protected <T> Stream<T> toStream(CloseableIterator<T> iterator) {
        Iterable<T> iterable = () -> iterator;
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    protected TableResult cleanupOnExit(TableResult tableResult) {
        cleanupList.add(tableResult);
        return tableResult;
    }
}
