package ebird2postgres;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import ebird2postgres.ebird.EBirdErrorHandler;
import ebird2postgres.ebird.EBirdReader;
import ebird2postgres.ebird.EBirdRecord;
import ebird2postgres.ebird.EBirdRecordHandler;
import ebird2postgres.locality.UrbanHotspots;
import ebird2postgres.repository.BirdSpecies;
import ebird2postgres.repository.BirdSpeciesRepository;
import ebird2postgres.repository.Checklist;
import ebird2postgres.repository.ChecklistRepository;
import ebird2postgres.repository.CityLocation;
import ebird2postgres.repository.Hotspot;
import ebird2postgres.repository.HotspotRepository;
import ebird2postgres.repository.ObservationRepository;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.singletonList;

public class Importer {
	private final static Logger LOGGER = LoggerFactory.getLogger(Importer.class);

	private final UrbanHotspots urbanHotspots = UrbanHotspots.urbanHotspots();
	private final BasicDataSource dataSource;

	private EBirdReader reader;
	
	public Importer(final InputStream eBird) throws Exception {
		reader = new EBirdReader(eBird);

		dataSource = new BasicDataSource();
		dataSource.setDriverClassName("org.postgresql.Driver");
		dataSource.setUrl("jdbc:postgresql://localhost/urban_ebird");
		dataSource.setDefaultAutoCommit(false);
		dataSource.setRollbackOnReturn(true);
	}
	
	public void importUrbanHotspots() {
		LOGGER.info("Reading in urban hotspots");
		final CityNameProvider urbanLocationProvider =
				localityId -> singletonList(new CityLocation(urbanHotspots.getCityName(localityId).get(), true));

		reader.read(localityId -> urbanHotspots.getCityName(localityId).isPresent(),
				new RecordHandler(urbanLocationProvider),
				new ErrorHandler());
	}
	
	public void shutdown() throws SQLException {
		dataSource.close();
	}
	
	private interface CityNameProvider {
		List<CityLocation> getCityLocations(String localityId);
	}
	
	private class RecordHandler implements EBirdRecordHandler {

		private final BirdSpeciesRepository birdSpeciesRepo = new BirdSpeciesRepository();
		private final ChecklistRepository checklistRepo =  new ChecklistRepository();
		private final HotspotRepository hotspotRepo = new HotspotRepository();
		private final ObservationRepository observationRepo = new ObservationRepository();
		private final CityNameProvider cityNameProvider;
		
		RecordHandler(final CityNameProvider cityNameProvider) {
			this.cityNameProvider = cityNameProvider;
		}
		
		@Override
		public void handle(EBirdRecord record) {
			try (final Connection connection = dataSource.getConnection()) {
				final List<CityLocation> cityLocations = cityNameProvider.getCityLocations(record.getLocalityId());
				final Hotspot hotspot = hotspotRepo.fetchHotspot(connection, record, cityLocations);
				final Checklist checklist = checklistRepo.fetchChecklist(connection, record, hotspot);
				final BirdSpecies birdSpecies = birdSpeciesRepo.fetchBirdSpecies(connection, record);

				LOGGER.debug("Storing new record {}", record.getId());
				observationRepo.store(connection, record, checklist, birdSpecies);

				connection.commit();
			} catch (Exception e) {
				throw new IllegalStateException("Failed to create entries from " + record, e);
			}
		}
	}
	
	private class ErrorHandler implements EBirdErrorHandler {

		@Override
		public void handleError(String lastRowRead, String[] currentRow, Exception error) {
			if (currentRow == null) {
				LOGGER.error("IO Error after reading row {}", lastRowRead);
			} else {
				LOGGER.error("Failed to process row {}", Arrays.toString(currentRow));
			}

			LOGGER.error("Error processing row", error);
		}
	}
}
