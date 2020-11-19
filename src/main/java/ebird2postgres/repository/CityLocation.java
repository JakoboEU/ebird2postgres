package ebird2postgres.repository;

public class CityLocation {
    private final String name;
    private final boolean isUrban;

    public CityLocation(String name, boolean isUrban) {
        this.name = name;
        this.isUrban = isUrban;
    }

    public String getName() {
        return name;
    }

    public boolean isUrban() {
        return isUrban;
    }

    @Override
    public String toString() {
        return "CityLocation{" +
                "name='" + name + '\'' +
                ", isUrban=" + isUrban +
                '}';
    }
}
