package ebird2postgres.ebird;

public interface EBirdErrorHandler {
	void handleError(String lastRowRead, String[] currentRow, Exception error);
}
