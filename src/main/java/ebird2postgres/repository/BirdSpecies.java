package ebird2postgres.repository;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;

import com.google.common.base.Objects;

import ebird2postgres.ebird.EBirdRecord;

public class BirdSpecies {

	private final Integer id;
	
	BirdSpecies(Integer id) {
		this.id = id;
	}
	
	public Integer getId() {
		return id;
	}

	public static BirdSpeciesName createBirdSpeciesName(EBirdRecord record) {
		return createBirdSpeciesName(record.getScientificName(), record.getCommonName(), record.getSubspeciesScientificName(), record.getSubspeciesCommonName());
	}
	
	private static BirdSpeciesName createBirdSpeciesName(String scientificName, String commonName, String subspeciesScientificName, String subspeciesCommonName) {
		if (subspeciesScientificName == null || subspeciesCommonName == null) {
			return new BirdSpeciesName(scientificName, commonName);
		} else {
			return new BirdSpeciesNameWithSubspecies(scientificName, commonName, subspeciesScientificName, subspeciesCommonName);
		}
	}
	
	public static BirdSpecies store(final Connection connection, final EBirdRecord record) throws SQLException {
		final BirdSpeciesName name = createBirdSpeciesName(record);
		return store(connection, name);
	}
	
	public static BirdSpecies store(final Connection connection, final BirdSpeciesName name) throws SQLException {
		final PreparedStatement ps = name.getStoreStatement(connection);
		ps.executeUpdate();
		final ResultSet rs = ps.getGeneratedKeys();
		
		if (rs.next()) {
			return new BirdSpecies(rs.getInt(1));
		} else {
			throw new IllegalStateException("Keys not generated");
		}
	}
	
	public static Optional<BirdSpecies> load(final Connection connection, final EBirdRecord record) throws SQLException {
		final BirdSpeciesName name = createBirdSpeciesName(record);
		return load(connection, name);
	}
	
	public static Optional<BirdSpecies> load(final Connection connection, final BirdSpeciesName name) throws SQLException {
		final PreparedStatement ps = name.getIdStatement(connection);
		
		final ResultSet rs = ps.executeQuery();
		
		if (rs.next()) {
			return Optional.of(new BirdSpecies(rs.getInt(1)));
		} else {
			return Optional.empty();
		}
	}
	
	public static class BirdSpeciesName {
		final String scientificName;
		final String commonName;
		final String subspeciesScientificName;
		final String subspeciesCommonName;
		
		BirdSpeciesName(String scientificName, String commonName, String subspeciesScientificName,
				String subspeciesCommonName) {
			this.scientificName = scientificName;
			this.commonName = commonName;
			this.subspeciesScientificName = subspeciesScientificName;
			this.subspeciesCommonName = subspeciesCommonName;
		}
		
		BirdSpeciesName(String scientificName, String commonName) {
			this(scientificName, commonName, null, null);
		}
		
		PreparedStatement getIdStatement(final Connection connection) throws SQLException {
			final PreparedStatement ps = connection.prepareStatement("SELECT id FROM bird_species WHERE scientific_name = ? AND common_name = ? AND subspecies_scientific_name IS NULL AND subspecies_common_name IS NULL");
			ps.setString(1, scientificName);
			ps.setString(2, commonName);
			return ps;
		}

		PreparedStatement getStoreStatement(final Connection connection) throws SQLException {
			final PreparedStatement ps = connection.prepareStatement("INSERT INTO bird_species (scientific_name, common_name) VALUES (?, ?)", Statement.RETURN_GENERATED_KEYS);
			ps.setString(1, scientificName);
			ps.setString(2, commonName);
			return ps;
		}
		
		public int hashCode() {
			return Objects.hashCode(scientificName, commonName, subspeciesScientificName, subspeciesCommonName);
		}
		
		public boolean equals(Object obj) {
			if (obj == null) {
				return false;
			} else if (obj == this) {
				return true;
			} else if (!(obj instanceof BirdSpeciesName)) {
				return false;
			} else {
				BirdSpeciesName that = (BirdSpeciesName) obj;
				return Objects.equal(this.commonName, that.commonName) && 
						Objects.equal(this.scientificName, that.scientificName) &&
						Objects.equal(this.subspeciesCommonName, that.subspeciesCommonName) &&
						Objects.equal(this.subspeciesScientificName, that.subspeciesScientificName);
			}
		}

		@Override
		public String toString() {
			return "BirdSpeciesName [scientificName=" + scientificName + ", commonName=" + commonName
					+ ", subspeciesScientificName=" + subspeciesScientificName + ", subspeciesCommonName="
					+ subspeciesCommonName + "]";
		}
	}
	
	public static class BirdSpeciesNameWithSubspecies extends BirdSpeciesName {
		
		public BirdSpeciesNameWithSubspecies(String scientificName, String commonName, String subspeciesScientificName,
				String subspeciesCommonName) {
			super(scientificName, commonName, subspeciesScientificName, subspeciesCommonName);
		}

		PreparedStatement getIdStatement(final Connection connection) throws SQLException {
			final PreparedStatement ps = connection.prepareStatement("SELECT id FROM bird_species WHERE scientific_name = ? AND common_name = ? AND subspecies_scientific_name = ? AND subspecies_common_name = ?");
			ps.setString(1, scientificName);
			ps.setString(2, commonName);
			ps.setString(3, subspeciesScientificName);
			ps.setString(4, subspeciesCommonName);
			return ps;
		}
		
		PreparedStatement getStoreStatement(final Connection connection) throws SQLException {
			final PreparedStatement ps = connection.prepareStatement("INSERT INTO bird_species(scientific_name, common_name, subspecies_scientific_name , subspecies_common_name) VALUES (?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
			ps.setString(1, scientificName);
			ps.setString(2, commonName);
			ps.setString(3, subspeciesScientificName);
			ps.setString(4, subspeciesCommonName);
			return ps;
		}
	}
}
