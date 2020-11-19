package ebird2postgres.repository;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import ebird2postgres.ebird.EBirdRecord;

public class Hotspot extends HotspotStub {
	private final String countryCode;
	private final String stateCode;
	private final String countyCode;
	private final String localityName;
	private final double latitude;
	private final double longitude;
	private final List<CityLocation> cities;
	
	Hotspot(final EBirdRecord record, final List<CityLocation> cities) {
		this(record.getCountryCode(), record.getStateCode(), record.getCountyCode(), record.getLocalityId(), record.getLocalityName(),
				record.getLatitude(), record.getLongitude(), cities);
	}
	
	private Hotspot(String countryCode, String stateCode, String countyCode, String localityId, String localityName,
			double latitude, double longitude, List<CityLocation> cities) {
		super(localityId);
		this.countryCode = countryCode;
		this.stateCode = stateCode;
		this.countyCode = countyCode;
		this.localityName = localityName;
		this.latitude = latitude;
		this.longitude = longitude;
		this.cities = cities;
	}
	
	public Hotspot insert(final Connection connection) throws SQLException {
		try(final PreparedStatement ps = connection.prepareStatement("INSERT INTO hotspot (locality_id, name, latitude, longitude, state_code, county_code, country_code) VALUES (?, ?, ?, ?, ?, ?, ?)")) {
			ps.setString(1, getLocalityId());
			ps.setString(2, localityName);
			ps.setDouble(3, latitude);
			ps.setDouble(4, longitude);
			ps.setString(5, stateCode);
			ps.setString(6, countyCode);
			ps.setString(7, countryCode);
			
			ps.executeUpdate();
		}

		try (final PreparedStatement ps = connection.prepareStatement("INSERT INTO hotspot_to_city (city, hotspot, is_urban) VALUES (?, ?, ?)")) {
			for (final CityLocation city : cities) {
				ps.setString(1, city.getName());
				ps.setString(2, getLocalityId());
				ps.setBoolean(3, city.isUrban());
				ps.executeUpdate();
			}
		}

		return this;
	}
	
	public static Optional<Hotspot> load(final Connection connection, final String localityId) throws SQLException {
		final List<CityLocation> cities = fetchCityNames(connection, localityId);

		try (final PreparedStatement ps = connection.prepareStatement("SELECT name, latitude, longitude, state_code, county_code, country_code FROM hotspot WHERE locality_id = ?")) {
			ps.setString(1, localityId.trim());
			
			final ResultSet rs = ps.executeQuery();
			
			if (rs.next()) {
				return Optional.of(
						new Hotspot(
								rs.getString("country_code"),
								rs.getString("state_code"),
								rs.getString("county_code"),
								localityId,
								rs.getString("name"),
								rs.getDouble("latitude"),
								rs.getDouble("longitude"),
								cities
						)
				);
			} else {
				return Optional.empty();
			}
		}
	}

	private static List<CityLocation> fetchCityNames(final Connection connection, final String localityId) throws SQLException {
		try (final PreparedStatement ps = connection.prepareStatement("SELECT city, is_urban FROM hotspot_to_city WHERE hotspot = ?")) {
			ps.setString(1, localityId);
			final ResultSet rs = ps.executeQuery();

			final List<CityLocation> result = new ArrayList<>();
			while (rs.next()) {
				result.add(new CityLocation(rs.getString(1), rs.getBoolean(2)));
			}

			return Collections.unmodifiableList(result);
		}
	}

	@Override
	public String toString() {
		return "Hotspot [localityId=" + getLocalityId() + ", countryCode=" + countryCode + ", stateCode=" + stateCode + ", countyCode=" + countyCode
				+ ", localityName=" + localityName + ", latitude=" + latitude + ", longitude=" + longitude
				+ ", cities=" + cities + "]";
	}

}
