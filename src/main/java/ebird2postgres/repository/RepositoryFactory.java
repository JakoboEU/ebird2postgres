package ebird2postgres.repository;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class RepositoryFactory {

	private final Connection connection;
	
	public RepositoryFactory() throws ClassNotFoundException, SQLException {
		Class.forName("org.postgresql.Driver");
		this.connection = DriverManager.getConnection("jdbc:postgresql://localhost/urban_ebird");
		this.connection.setAutoCommit(false);
	}
	
	public BirdSpeciesRepository birdSpeciesRepository() {
		return new BirdSpeciesRepository(connection);
	}
	
	public ChecklistRepository checklistRepository() {
		return new ChecklistRepository(connection);
	}
	
	public HotspotRepository hotsportRepository() {
		return new HotspotRepository(connection);
	}
	
	public ObservationRepository observationRepository() {
		return new ObservationRepository(connection);
	}
	
	public void shutdown() throws SQLException {
		commit();
		connection.close();
	}

	public void commit() throws SQLException {
		connection.commit();
	}
	
	public void rollback() throws SQLException {
		connection.rollback();
	}
}
