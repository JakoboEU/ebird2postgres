package ebird2postgres;

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
import ebird2postgres.repository.RepositoryFactory;

import static java.util.Collections.singletonList;

public class Importer {

	private final RepositoryFactory repositoryFactory = new RepositoryFactory();
	private final UrbanHotspots urbanHotspots = UrbanHotspots.urbanHotspots();
	private EBirdReader reader;
	
	public Importer(final String filename) throws Exception {
		reader = new EBirdReader(filename);
	}
	
	public void importUrbanHotspots() {
		final CityNameProvider urbanLocationProvider =
				localityId -> singletonList(new CityLocation(urbanHotspots.getCityName(localityId).get(), true));

		reader.read(localityId -> urbanHotspots.getCityName(localityId).isPresent(),
				new RecordHandler(urbanLocationProvider),
				new ErrorHandler());
	}
	
	public void shutdown() throws SQLException {
		repositoryFactory.shutdown();
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			System.out.println("EBird tsv must be specified as argument");
			System.exit(-1);
		}

		final Importer importer = new Importer(args[0]);
		importer.importUrbanHotspots();
		importer.shutdown();
	}
	
	private interface CityNameProvider {
		List<CityLocation> getCityLocations(String localityId);
	}
	
	private class RecordHandler implements EBirdRecordHandler {

		private final BirdSpeciesRepository birdSpeciesRepo = repositoryFactory.birdSpeciesRepository();
		private final ChecklistRepository checklistRepo = repositoryFactory.checklistRepository();
		private final HotspotRepository hotspotRepo = repositoryFactory.hotsportRepository();
		private final ObservationRepository observationRepo = repositoryFactory.observationRepository();
		private final CityNameProvider cityNameProvider;
		
		RecordHandler(final CityNameProvider cityNameProvider) {
			this.cityNameProvider = cityNameProvider;
		}
		
		@Override
		public void handle(EBirdRecord record) {
			try {
				final List<CityLocation> cityLocations = cityNameProvider.getCityLocations(record.getLocalityId());
				final Hotspot hotspot = hotspotRepo.fetchHotspot(record, cityLocations);
				final Checklist checklist = checklistRepo.fetchChecklist(record, hotspot);
				final BirdSpecies birdSpecies = birdSpeciesRepo.fetchBirdSpecies(record);
				
				observationRepo.store(record, checklist, birdSpecies);
				
				repositoryFactory.commit();
			} catch (Exception e) {
				try {
					repositoryFactory.rollback();
				} catch (SQLException rollbackError) {
					System.err.println("Failed to rollback transaction");
					rollbackError.printStackTrace();
				}
				throw new IllegalStateException("Failed to create entries from " + record, e);
			}
		}
	}
	
	private class ErrorHandler implements EBirdErrorHandler {

		@Override
		public void handleError(String lastRowRead, String[] currentRow, Exception error) {
			if (currentRow == null) {
				System.err.println("IO Error after reading row " + lastRowRead);
			} else {
				System.err.println("Failed to process row " + Arrays.toString(currentRow));
			}
			
			error.printStackTrace();
		}
	}
}
