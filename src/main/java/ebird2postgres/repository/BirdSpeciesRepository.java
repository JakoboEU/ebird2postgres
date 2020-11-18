package ebird2postgres.repository;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import ebird2postgres.ebird.EBirdRecord;
import ebird2postgres.repository.BirdSpecies.BirdSpeciesName;

public class BirdSpeciesRepository {
	private final LoadingCache<BirdSpecies.BirdSpeciesName, BirdSpecies> cache = CacheBuilder
			.newBuilder().maximumSize(5000).build(new CacheLoader<BirdSpecies.BirdSpeciesName, BirdSpecies>() {
				@Override
				public BirdSpecies load(BirdSpeciesName key) throws Exception {
					return BirdSpecies.load(connection, key)
							.orElseGet(() -> {
								try {
									return BirdSpecies.store(connection, key);
								} catch (SQLException e) {
									throw new IllegalStateException("Failed to create bird species " + key, e);
								}
							});
				}
			});
	
	private final Connection connection;
	
	public BirdSpeciesRepository(final Connection connection) {
		this.connection = connection;
	}
	
	public BirdSpecies fetchBirdSpecies(final EBirdRecord ebirdRecord) throws ExecutionException {
		final BirdSpeciesName name = BirdSpecies.createBirdSpeciesName(ebirdRecord);
		return cache.get(name);
	}
}
