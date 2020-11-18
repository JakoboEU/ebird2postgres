package ebird2postgres.locality;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

public final class UrbanHotspots {

	private final Map<String,String> hotspotsToCity;
	
	private UrbanHotspots(final Map<String,String> hotspotsToCity) {
		this.hotspotsToCity = hotspotsToCity;
	}
	
	public static UrbanHotspots urbanHotspots() throws IOException {
		try (Reader inputReader = new InputStreamReader(UrbanHotspots.class.getResourceAsStream("urban_hotspots.csv"), "UTF-8")) {
		    final CsvParser parser = new CsvParser(new CsvParserSettings());
		    final Stream<String[]> parsedRows = parser.parseAll(inputReader).stream();
		    
		    final Map<String,String> hotspotsToCity = parsedRows.collect(Collectors.toConcurrentMap(s -> s[0], s -> s[5]));
		    
		    return new UrbanHotspots(hotspotsToCity);
		}
	}
	
	public Optional<String> getCityName(final String localityId) {
		if (this.hotspotsToCity.containsKey(localityId)) {
			return Optional.of(this.hotspotsToCity.get(localityId));
		}
		
		return Optional.empty();
	}
	
	public static void main(final String[] args) throws IOException {
		System.out.println(UrbanHotspots.urbanHotspots().getCityName("L247929").get());
	}
}
