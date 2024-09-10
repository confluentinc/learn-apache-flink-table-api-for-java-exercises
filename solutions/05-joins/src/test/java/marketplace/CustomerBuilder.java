package marketplace;

import org.apache.flink.types.Row;

import java.util.Random;

class CustomerBuilder {
    private int customerId;
    private String name;
    private String address;
    private String postCode;
    private String city;
    private String email;

    private final Random rnd = new Random(System.currentTimeMillis());

    public CustomerBuilder() {
        customerId = rnd.nextInt(1000);
        name = "Name" + rnd.nextInt(1000);
        address = "Address" + rnd.nextInt(1000);
        postCode = "PostCode" + rnd.nextInt(1000);
        city = "City" + rnd.nextInt(1000);
        email = "Email" + rnd.nextInt(1000);
    }

    public CustomerBuilder withCustomerId(int customerId) {
        this.customerId = customerId;
        return this;
    }

    public CustomerBuilder withName(String name) {
        this.name = name;
        return this;
    }

    public CustomerBuilder withAddress(String address) {
        this.address = address;
        return this;
    }

    public CustomerBuilder withPostCode(String postCode) {
        this.postCode = postCode;
        return this;
    }

    public CustomerBuilder withCity(String city) {
        this.city = city;
        return this;
    }

    public CustomerBuilder withEmail(String email) {
        this.email = email;
        return this;
    }

    public Row build() {
        return Row.of(customerId, name, address, postCode, city, email);
    }
}
