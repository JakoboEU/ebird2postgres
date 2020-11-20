package ebird2postgres.repository;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import ebird2postgres.ebird.EBirdRecord;

public class HotspotRepository {

	private final Cache<String, Hotspot> cache = CacheBuilder
			.newBuilder().maximumSize(2000). <String, Hotspot> build();

	public Hotspot fetchHotspot(final Connection connection, final EBirdRecord ebirdRecord, final List<CityLocation> cityLocations) throws ExecutionException {
		return cache.get(ebirdRecord.getLocalityId(), () -> loadHotspot(connection, ebirdRecord, cityLocations));
	}
	
	private Hotspot loadHotspot(final Connection connection, final EBirdRecord ebirdRecord, final List<CityLocation> cityLocations) throws SQLException {
		return Hotspot.load(connection, ebirdRecord.getLocalityId())
				.orElseGet(() -> {
					try {
						return new Hotspot(ebirdRecord, cityLocations).insert(connection);
					} catch (SQLException e) {
						throw new IllegalStateException("Failed to create hotspot from " + ebirdRecord, e);
					}
				});
	}
}
