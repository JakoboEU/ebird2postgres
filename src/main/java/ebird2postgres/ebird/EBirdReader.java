package ebird2postgres.ebird;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.concurrent.atomic.AtomicReference;

import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;

public class EBirdReader {
	private final static Logger LOGGER = LoggerFactory.getLogger(EBirdReader.class);

	private final AtomicReference<String> lastRowRead = new AtomicReference<String>();

	private final InputStream ebird;

	public EBirdReader(final InputStream ebird) {
		this.ebird = ebird;
	}
	
	public void read(final EBirdLocalityPredicate predicate, final EBirdRecordHandler recordHandler, final EBirdErrorHandler errorHandler) {
		TsvParserSettings settings = new TsvParserSettings();
		settings.setHeaderExtractionEnabled(true);
		TsvParser parser = new TsvParser(settings);

		parser.iterate(ebird, "UTF-8").forEach(row -> {
			final String localityId = row[23];
			if (predicate.accept(localityId)) {
				try {
					LOGGER.trace("Added record {} to queue.", row[0]);
					recordHandler.handle(new EBirdRecord(row));
				} catch (Exception e) {
					errorHandler.handleError(lastRowRead.get(), row, e);
				}
			}
		});
	}
}
