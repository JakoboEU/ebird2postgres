package ebird2postgres.ebird;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;

public class EBirdReader {
	
	private final AtomicReference<String> lastRowRead = new AtomicReference<String>();

	private final String ebirdFileName;
	
	public EBirdReader(final String ebirdFileName) {
		this.ebirdFileName = ebirdFileName;
	}
	
	public void read(final EBirdLocalityPredicate predicate, final EBirdRecordHandler recordHandler, final EBirdErrorHandler errorHandler) {
		TsvParserSettings settings = new TsvParserSettings();
		settings.setHeaderExtractionEnabled(true);
		TsvParser parser = new TsvParser(settings);
		
		try (InputStream is = new BufferedInputStream(new FileInputStream(new File(ebirdFileName)))) {
			parser.iterate(is, "UTF-8").forEach(row -> handleRow(row, predicate, recordHandler, errorHandler));
		} catch (IOException e) {
			errorHandler.handleError(lastRowRead.get(), null, e);
		}
	}	

	private void handleRow(final String[] row, final EBirdLocalityPredicate predicate, final EBirdRecordHandler recordHandler, final EBirdErrorHandler errorHandler) {
		final String previousRow = lastRowRead.getAndSet(row[0]);
		
		final String localityId = row[23];
		
		if (predicate.accept(localityId)) {
			try {
				recordHandler.handle(new EBirdRecord(row));
			} catch (Exception e) {
				errorHandler.handleError(previousRow, row, e);
			}
		}
	}
	
	public static void main(final String[] args) {
		final EBirdReader eb = new EBirdReader("/Users/jamesr/Dropbox/PhD/eBird/ebd_relAug-2020.txt");
		
		eb.read(e -> true, e -> System.out.println(e), (a, row, t) -> {System.out.println(Arrays.toString(row)); t.printStackTrace();});
	}
}
