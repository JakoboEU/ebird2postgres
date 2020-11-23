package ebird2postgres.ebird;

import java.io.InputStream;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;
import ebird2postgres.log.Logger;
import ebird2postgres.log.LoggerFactory;

public class EBirdReader {
	private final static Logger LOGGER = LoggerFactory.getLogger(EBirdReader.class);

	private final InputStream ebird;

	private final AtomicBoolean finishedReadingEBird = new AtomicBoolean(false);

	private final Executor producerThread = Executors.newFixedThreadPool(2);

	private final BlockingQueue<String[]> rowQueue = new ArrayBlockingQueue<>(1024);

	private final BlockingQueue<EBirdRecord> recordQueue = new ArrayBlockingQueue<>(1024);

	public EBirdReader(final InputStream ebird) {
		this.ebird = ebird;
	}
	
	public void read(final EBirdLocalityPredicate predicate, final EBirdRecordHandler recordHandler, final EBirdErrorHandler errorHandler) {
		producerThread.execute(new RowProducer(errorHandler));
		producerThread.execute(new EBirdRecordProducer(predicate, errorHandler));

		final AtomicReference<String> lastRowRead = new AtomicReference<String>();
		while (!finishedReadingEBird.get() || !rowQueue.isEmpty() || !recordQueue.isEmpty()) {
			try {
				final EBirdRecord record = recordQueue.take();
				LOGGER.debug("Storing {0}", record.getId());
				recordHandler.handle(record);
				lastRowRead.set(record.getId());
			} catch (InterruptedException e) {
				errorHandler.handleError(lastRowRead.get(), null, e);
			}
		}
	}

	private final class EBirdRecordProducer implements Runnable {
		private final EBirdLocalityPredicate predicate;
		private final EBirdErrorHandler errorHandler;

		private EBirdRecordProducer(final EBirdLocalityPredicate predicate, final EBirdErrorHandler errorHandler) {
			this.predicate = predicate;
			this.errorHandler = errorHandler;
		}

		@Override
		public void run() {
			final AtomicReference<String> lastRowRead = new AtomicReference<String>();

			while (!finishedReadingEBird.get()) {
				try {
					final String[] take = rowQueue.take();
					lastRowRead.set(take[0]);
					handleRow(predicate, errorHandler, take);
				} catch (InterruptedException e) {
					errorHandler.handleError(lastRowRead.get(), null, e);
				}
			}
		}

		private void handleRow(final EBirdLocalityPredicate predicate, final EBirdErrorHandler errorHandler, final String[] row) {
			try {
				if (row.length < 43) {
					errorHandler.handleError(null, row, new RuntimeException("Invalid row length; " + row.length));
					return;
				}

				final String localityId = row[23];

				if (localityId == null) {
					errorHandler.handleError(null, row, new RuntimeException("localityId is null"));
					return;
				}

				LOGGER.trace("Item {0} read.", row[0]);
				if (predicate.accept(localityId)) {
					LOGGER.trace("Keep {0}.", row[0]);
					recordQueue.put(new EBirdRecord(row));
				}
			} catch (Exception e) {
				errorHandler.handleError(null, row, e);
			}
		}
	}

	private final class RowProducer implements Runnable {

		private final EBirdErrorHandler errorHandler;

		private RowProducer(final EBirdErrorHandler errorHandler) {
			this.errorHandler = errorHandler;
		}

		@Override
		public void run() {
			TsvParserSettings settings = new TsvParserSettings();
			settings.setHeaderExtractionEnabled(true);
			TsvParser parser = new TsvParser(settings);

			final AtomicReference<String> lastRowRead = new AtomicReference<String>();

			parser.iterate(ebird, "UTF-8").forEach(row -> {
				try {
					LOGGER.trace("Adding {0}.", row[0]);
					rowQueue.put(row);
					lastRowRead.set(row[0]);
				} catch (Exception e) {
					errorHandler.handleError(lastRowRead.get(), null, e);
				}
			});

			LOGGER.info("Finished reading eBird file");

			finishedReadingEBird.set(true);
		}
	}
}
