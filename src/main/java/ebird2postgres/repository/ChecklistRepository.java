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
    
	private final Connection connection;
	
	public ChecklistRepository(final Connection connection) {
		this.connection = connection;
	}

	public Checklist fetchChecklist(final EBirdRecord ebirdRecord, final Hotspot hotspot) throws ExecutionException {
		return cache.get(ebirdRecord.getChecklistId(), () -> loadChecklist(ebirdRecord, hotspot));
	}
	
	private Checklist loadChecklist(final EBirdRecord ebirdRecord, final Hotspot hotspot) throws SQLException {
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
