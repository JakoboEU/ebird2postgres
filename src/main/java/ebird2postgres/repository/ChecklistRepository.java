package ebird2postgres.repository;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import ebird2postgres.ebird.EBirdRecord;

public class ChecklistRepository {

	private final Cache<String, Checklist> cache = CacheBuilder
			.newBuilder().maximumSize(2000). <String, Checklist> build();

	public Checklist fetchChecklist(final Connection connection, final EBirdRecord ebirdRecord, final Hotspot hotspot) throws ExecutionException {
		return cache.get(ebirdRecord.getChecklistId(), () -> loadChecklist(connection, ebirdRecord, hotspot));
	}
	
	private Checklist loadChecklist(final Connection connection, final EBirdRecord ebirdRecord, final Hotspot hotspot) throws SQLException {
		return Checklist.load(connection, ebirdRecord.getChecklistId())
				.orElseGet(() -> {
					try {
						return new Checklist(ebirdRecord, hotspot).insert(connection);
					} catch (SQLException e) {
						throw new IllegalStateException("Failed to create checklist from " + ebirdRecord, e);
					}
				});
	}
}
