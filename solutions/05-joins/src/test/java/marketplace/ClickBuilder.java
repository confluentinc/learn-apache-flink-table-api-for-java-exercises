package marketplace;

import org.apache.flink.types.Row;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Random;

public class ClickBuilder {
    private String clickId;
    private int userId;
    private String url;
    private String userAgent;
    private int viewTime;
    private Instant timestamp;

    public ClickBuilder() {
        Random rnd = new Random();

        clickId = "Click"+rnd.nextInt(1000);
        userId = rnd.nextInt(1000);
        url = "http://some.url/"+rnd.nextInt(1000);
        userAgent = "UserAgent"+rnd.nextInt(1000);
        viewTime = rnd.nextInt(1000);
        timestamp = Instant.now().truncatedTo( ChronoUnit.MILLIS );
    }

    public ClickBuilder withClickId(String clickId) {
        this.clickId = clickId;
        return this;
    }

    public ClickBuilder withUserId(int userId) {
        this.userId = userId;
        return this;
    }

    public ClickBuilder withUrl(String url) {
        this.url = url;
        return this;
    }

    public ClickBuilder withViewTime(int viewTime) {
        this.viewTime = viewTime;
        return this;
    }

    public ClickBuilder withUserAgent(String user_agent) {
        this.userAgent = user_agent;
        return this;
    }

    public ClickBuilder withTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public Row build() {
        return Row.of(clickId, userId, url, userAgent, viewTime, timestamp);
    }
}
