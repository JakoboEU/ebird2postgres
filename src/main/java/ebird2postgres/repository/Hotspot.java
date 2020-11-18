package ebird2postgres.repository;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

import ebird2postgres.ebird.EBirdRecord;

public class Hotspot extends HotspotStub {
	private final String countryCode;
	private final String stateCode;
	private final String countyCode;
	private final String localityName;
	private final double latitude;
	private final double longitude;
	private final boolean isUrban;
	private final String cityName;
	
	Hotspot(final EBirdRecord record, final String cityName, final boolean isUrban) {
		this(record.getCountryCode(), record.getStateCode(), record.getCountyCode(), record.getLocalityId(), record.getLocalityName(),
				record.getLatitude(), record.getLongitude(), isUrban, cityName);
	}
	
	private Hotspot(String countryCode, String stateCode, String countyCode, String localityId, String localityName,
			double latitude, double longitude, boolean isUrban, String cityName) {
		super(localityId);
		this.countryCode = countryCode;
		this.stateCode = stateCode;
		this.countyCode = countyCode;
		this.localityName = localityName;
		this.latitude = latitude;
		this.longitude = longitude;
		this.isUrban = isUrban;
		this.cityName = cityName;
	}
	
	public Hotspot insert(final Connection connection) throws SQLException {
		try(final PreparedStatement ps = connection.prepareStatement("INSERT INTO hotspot (locality_id, city, name, latitude, longitude, state_code, county_code, country_code, is_urban) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
			ps.setString(1, getLocalityId());
			ps.setString(2, cityName);
			ps.setString(3, localityName);
			ps.setDouble(4, latitude);
			ps.setDouble(5, longitude);
			ps.setString(6, stateCode);
			ps.setString(7, countyCode);
			ps.setString(8, countryCode);
			ps.setBoolean(9, isUrban);
			
			ps.executeUpdate();
			
			return this;
		}
	}
	
	public static Optional<Hotspot> load(final Connection connection, final String localityId) throws SQLException {
		try (final PreparedStatement ps = connection.prepareStatement("SELECT city, name, latitude, longitude, state_code, county_code, country_code, is_urban FROM hotspot WHERE locality_id = ?")) {
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
								rs.getBoolean("is_urban"),
								rs.getString("city")
						)
				);
			} else {
				return Optional.empty();
			}
		}
	}

	@Override
	public String toString() {
		return "Hotspot [localityId=" + getLocalityId() + ", countryCode=" + countryCode + ", stateCode=" + stateCode + ", countyCode=" + countyCode
				+ ", localityName=" + localityName + ", latitude=" + latitude + ", longitude=" + longitude
				+ ", isUrban=" + isUrban + ", cityName=" + cityName + "]";
	}
}
