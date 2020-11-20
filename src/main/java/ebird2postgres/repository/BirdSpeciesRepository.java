package ebird2postgres.repository;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import ebird2postgres.ebird.EBirdRecord;
import ebird2postgres.repository.BirdSpecies.BirdSpeciesName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BirdSpeciesRepository {
	private final static Logger LOGGER = LoggerFactory.getLogger(BirdSpeciesRepository.class);

	private final Cache<BirdSpecies.BirdSpeciesName, BirdSpecies> cache = CacheBuilder
			.newBuilder()
			.maximumSize(6000)
			.removalListener(n -> LOGGER.trace("Removing {} from cache", n.getValue()))
			. <BirdSpecies.BirdSpeciesName, BirdSpecies> build();

	public BirdSpecies fetchBirdSpecies(final Connection connection, final EBirdRecord ebirdRecord) throws ExecutionException {
		final BirdSpeciesName name = BirdSpecies.createBirdSpeciesName(ebirdRecord);
		return cache.get(name, () -> loadBirdSpecies(connection, name));
	}

	private BirdSpecies loadBirdSpecies(Connection connection, BirdSpeciesName name) throws SQLException {
		return BirdSpecies.load(connection, name)
				.orElseGet(() -> {
					try {
						return BirdSpecies.store(connection, name);
					} catch (SQLException e) {
						throw new IllegalStateException("Failed to create bird species " + name, e);
					}
				});
	}
}
