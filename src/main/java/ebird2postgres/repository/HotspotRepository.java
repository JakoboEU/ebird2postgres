package ebird2postgres.repository;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import ebird2postgres.ebird.EBirdRecord;
import ebird2postgres.log.Logger;
import ebird2postgres.log.LoggerFactory;

public class HotspotRepository {
	private final static Logger LOGGER = LoggerFactory.getLogger(HotspotRepository.class);

	private final Cache<String, Hotspot> cache = CacheBuilder
			.newBuilder()
			.maximumSize(5000)
			.removalListener(n -> LOGGER.trace("Removing {0} from cache", n.getKey()))
			. <String, Hotspot> build();

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
