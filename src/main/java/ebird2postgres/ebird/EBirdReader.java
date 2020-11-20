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

	private final InputStream ebird;
	
	public EBirdReader(final InputStream ebird) {
		this.ebird = ebird;
	}
	
	public void read(final EBirdLocalityPredicate predicate, final EBirdRecordHandler recordHandler, final EBirdErrorHandler errorHandler) {
		TsvParserSettings settings = new TsvParserSettings();
		settings.setHeaderExtractionEnabled(true);
		TsvParser parser = new TsvParser(settings);
		
		try (InputStream is = new BufferedInputStream(ebird)) {
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
}
