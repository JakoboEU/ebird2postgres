package ebird2postgres.repository;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import ebird2postgres.ebird.EBirdRecord;
import ebird2postgres.log.Logger;
import ebird2postgres.log.LoggerFactory;

public class ObservationRepository {

	private final static Logger LOGGER = LoggerFactory.getLogger(HotspotRepository.class);

	public void store(final Connection connection, final EBirdRecord record, final Checklist checklist, final BirdSpecies birdSpecies) throws SQLException {
		try (final PreparedStatement ps = connection.prepareStatement("SELECT id FROM observation WHERE id = ?")) {
			ps.setString(1, record.getId());
			final ResultSet rs = ps.executeQuery();
			if (rs.next()) {
				LOGGER.debug("Observation {0} already exists.", record.getId());
				return;
			}
		}
		
		try (final PreparedStatement ps = connection.prepareStatement("INSERT INTO observation (id, bird_species, count, count_provided, approved, reason, checklist) VALUES (?, ?, ?, ?, ?, ?, ?)")) {
			ps.setString(1, record.getId());
			ps.setInt(2, birdSpecies.getId());
			ps.setObject(3, record.getObservationCount());
			ps.setBoolean(4, record.getObservationCount() != null);
			ps.setBoolean(5, record.isRecordApproved());
			ps.setString(6, record.getApprovalReason());
			ps.setString(7, checklist.getChecklistId());
			ps.executeUpdate();
		}
	}
}
