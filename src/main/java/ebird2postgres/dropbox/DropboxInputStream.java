package ebird2postgres.dropbox;

import com.dropbox.core.DbxException;
import com.dropbox.core.v2.DbxClientV2;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

public class DropboxInputStream extends InputStream {

    private final DbxClientV2 client;
    private final String downloadFileName;
    private final int chunkSize;

    private InputStream inputStream;
    private int nextChunk;

    public DropboxInputStream(final DbxClientV2 client, final String fileName, final int chunkSize) {
        this.client = client;
        this.downloadFileName = fileName;
        this.chunkSize = chunkSize;
    }

    @Override
    public int read() throws IOException {
        int read;
        if (inputStream == null || (read = inputStream.read()) == -1) {
            final int startChunk = nextChunk;
            nextChunk = startChunk + chunkSize;

            try {
                this.inputStream = new BufferedInputStream(
                        client.files()
                                .downloadBuilder(downloadFileName)
                                .range(startChunk, nextChunk)
                                .start()
                                .getInputStream());
            } catch (DbxException e) {
                throw new IOException(e);
            }

            read = this.inputStream.read();
        }

        return read;
    }
}
