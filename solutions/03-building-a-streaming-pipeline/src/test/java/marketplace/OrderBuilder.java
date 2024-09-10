package marketplace;

import org.apache.flink.types.Row;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Random;

class OrderBuilder {
    private String orderId, productId;
    private Integer customerId;
    private Double price;
    private Instant timestamp;

    public OrderBuilder() {
        Random rnd = new Random();
        orderId = "Order" + rnd.nextInt(1000);
        customerId = rnd.nextInt(1000);
        productId = "Product" + rnd.nextInt(1000);
        price = rnd.nextDouble(100);
        timestamp = Instant.now().truncatedTo( ChronoUnit.MILLIS );
    }

    public OrderBuilder withOrderId(String orderId) {
        this.orderId = orderId;
        return this;
    }

    public OrderBuilder withCustomerId(int customerId) {
        this.customerId = customerId;
        return this;
    }

    public OrderBuilder withProductId(String productId) {
        this.productId = productId;
        return this;
    }

    public OrderBuilder withPrice(Double price) {
        this.price = price;
        return this;
    }

    public OrderBuilder withTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public Row build() {
        return Row.of(orderId, customerId, productId, price, timestamp);
    }
}
