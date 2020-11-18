package ebird2postgres;

import java.sql.SQLException;
import java.util.Arrays;

import ebird2postgres.ebird.EBirdErrorHandler;
import ebird2postgres.ebird.EBirdReader;
import ebird2postgres.ebird.EBirdRecord;
import ebird2postgres.ebird.EBirdRecordHandler;
import ebird2postgres.locality.UrbanHotspots;
import ebird2postgres.repository.BirdSpecies;
import ebird2postgres.repository.BirdSpeciesRepository;
import ebird2postgres.repository.Checklist;
import ebird2postgres.repository.ChecklistRepository;
import ebird2postgres.repository.Hotspot;
import ebird2postgres.repository.HotspotRepository;
import ebird2postgres.repository.ObservationRepository;
import ebird2postgres.repository.RepositoryFactory;

public class Importer {

	private final RepositoryFactory repositoryFactory = new RepositoryFactory();
	private final UrbanHotspots urbanHotspots = UrbanHotspots.urbanHotspots();
	private EBirdReader reader;
	
	public Importer(final String filename) throws Exception {
		reader = new EBirdReader(filename);
	}
	
	public void importUrbanHotspots() {
		reader.read(localityId -> urbanHotspots.getCityName(localityId).isPresent(), 
				new RecordHandler(true, localityId -> urbanHotspots.getCityName(localityId).get()), 
				new ErrorHandler());
	}
	
	public void shutdown() throws SQLException {
		repositoryFactory.shutdown();
	}
	
	public static void main(String[] args) throws Exception {
		final Importer importer = new Importer("/Users/jamesr/Dropbox/PhD/eBird/ebd_relAug-2020.txt");
		importer.importUrbanHotspots();
		importer.shutdown();
	}
	
	private interface CityNameProvider {
		String getCityName(String localityId);
	}
	
	private class RecordHandler implements EBirdRecordHandler {

		private final BirdSpeciesRepository birdSpeciesRepo = repositoryFactory.birdSpeciesRepository();
		private final ChecklistRepository checklistRepo = repositoryFactory.checklistRepository();
		private final HotspotRepository hotspotRepo = repositoryFactory.hotsportRepository();
		private final ObservationRepository observationRepo = repositoryFactory.observationRepository();
		private final boolean isUrban;
		private final CityNameProvider cityNameProvider;
		
		RecordHandler(final boolean isUrban, final CityNameProvider cityNameProvider) {
			this.isUrban = isUrban;
			this.cityNameProvider = cityNameProvider;
		}
		
		@Override
		public void handle(EBirdRecord record) {
			try {
				final String cityName = cityNameProvider.getCityName(record.getLocalityId());
				final Hotspot hotspot = hotspotRepo.fetchHotspot(record, cityName, this.isUrban);
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
