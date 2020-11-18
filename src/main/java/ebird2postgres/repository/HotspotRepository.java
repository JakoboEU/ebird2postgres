package ebird2postgres.repository;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import ebird2postgres.ebird.EBirdRecord;

public class HotspotRepository {

	private final Cache<String, Hotspot> cache = CacheBuilder
			.newBuilder().maximumSize(2000). <String, Hotspot> build();
    
	private final Connection connection;
	
	
	public HotspotRepository(final Connection connection) {
		this.connection = connection;
	}

	public Hotspot fetchHotspot(final EBirdRecord ebirdRecord, final String cityName, final boolean isUrban) throws ExecutionException {
		return cache.get(ebirdRecord.getLocalityId(), () -> loadHotspot(ebirdRecord, cityName, isUrban));
	}
	
	private Hotspot loadHotspot(final EBirdRecord ebirdRecord, final String cityName, final boolean isUrban) throws SQLException {
		return Hotspot.load(connection, ebirdRecord.getLocalityId())
				.orElseGet(() -> {
					try {
						return new Hotspot(ebirdRecord, cityName, isUrban).insert(connection);
					} catch (SQLException e) {
						throw new IllegalStateException("Failed to create hotspot from " + ebirdRecord, e);
					}
				});
	}
}
