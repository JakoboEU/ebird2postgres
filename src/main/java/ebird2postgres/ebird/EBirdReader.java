package ebird2postgres.ebird;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicReference;

import com.univocity.parsers.common.processor.BatchedColumnProcessor;
import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;

public class EBirdReader {

	private static final int MAX_QUEUE_SIZE = 100;

	private final AtomicReference<String> lastRowRead = new AtomicReference<String>();

	private final InputStream ebird;

	private final BlockingQueue<String[]> queue = new LinkedBlockingDeque<>(MAX_QUEUE_SIZE);

	private final ExecutorService executorService = Executors.newFixedThreadPool(3);

	public EBirdReader(final InputStream ebird) {
		this.ebird = ebird;
	}
	
	public void read(final EBirdLocalityPredicate predicate, final EBirdRecordHandler recordHandler, final EBirdErrorHandler errorHandler) {
		startReading(errorHandler);
		startWriting(predicate, recordHandler, errorHandler);
	}

	private void startReading(final EBirdErrorHandler errorHandler) {
		TsvParserSettings settings = new TsvParserSettings();
		settings.setHeaderExtractionEnabled(true);
		TsvParser parser = new TsvParser(settings);

		executorService.submit(() -> {
			try (InputStream is = new BufferedInputStream(ebird)) {
				parser.iterate(is, "UTF-8").forEach(row -> {
					try {
						queue.put(row);
					} catch (InterruptedException e) {
						errorHandler.handleError(lastRowRead.get(), row, e);
					}
				});
			} catch (IOException e) {
				errorHandler.handleError(lastRowRead.get(), null, e);
			}
			System.out.println("Finished reading file.");
		});
	}

	private void startWriting(final EBirdLocalityPredicate predicate, final EBirdRecordHandler recordHandler, final EBirdErrorHandler errorHandler) {
		executorService.submit(() -> {
			try {
				handleRow(queue.take(), predicate, recordHandler, errorHandler);
			} catch (InterruptedException e) {
				errorHandler.handleError(lastRowRead.get(), null, e);
			}
		});
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
