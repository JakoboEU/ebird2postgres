package ebird2postgres.dropbox;

import com.dropbox.core.DbxException;
import com.dropbox.core.v2.DbxClientV2;
import ebird2postgres.Application;
import ebird2postgres.log.Logger;
import ebird2postgres.log.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

public class DropboxInputStream extends InputStream {
    private final static Logger LOGGER = LoggerFactory.getLogger(Application.class);

    private final DbxClientV2 client;
    private final String downloadFileName;
    private final int chunkSize;

    private InputStream inputStream;
    private long nextChunk;

    public DropboxInputStream(final DbxClientV2 client, final String fileName, final int chunkSize) throws IOException {
        this.client = client;
        this.downloadFileName = fileName;
        this.chunkSize = chunkSize;
        this.inputStream = getNextChunk();
    }

    @Override
    public int read() throws IOException {
        final int read = this.inputStream.read();
        if (read != -1) {
            return read;
        }

        this.inputStream = getNextChunk();
        return read();
    }

    private InputStream getNextChunk() throws IOException {
        final long startChunk = nextChunk;
        nextChunk = startChunk + chunkSize;

        LOGGER.debug("Creating stream for next chunk from {0}", startChunk);

        try {
            return new BufferedInputStream(client.files()
                        .downloadBuilder(downloadFileName)
                        .range(startChunk, chunkSize)
                        .start()
                        .getInputStream(), chunkSize);
        } catch (DbxException e) {
            throw new IOException(e);
        }
    }
}
