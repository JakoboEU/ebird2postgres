package ebird2postgres;

import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.Metadata;
import ebird2postgres.dropbox.DropboxInputStream;
import ebird2postgres.log.Logger;
import ebird2postgres.log.LoggerFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.util.Optional;

public class Application {

    private final static Logger LOGGER = LoggerFactory.getLogger(Application.class);

    private final static Options COMMAND_LINE_OPTIONS = new Options();

    private final static String EBIRD_FILE_OPTION = "f";
    private final static String DROPBOX_ACCESS_TOKEN = "a";

    // approx 300MB                                   2147483648
    private final static int DROPBOX_READ_CHUNK_SIZE = 52428800;

    private final static long DROPBOX_START_READING = 87356866560l;

    static {
        COMMAND_LINE_OPTIONS.addOption(Option.builder(EBIRD_FILE_OPTION).argName("eBird filename in DropBox").hasArg(true).required().build());
        COMMAND_LINE_OPTIONS.addOption(Option.builder(DROPBOX_ACCESS_TOKEN).argName("DropBox Access Token").hasArg(true).required().build());
    }

    public static void main(String[] args) throws Exception {
        final DefaultParser commandLine = new DefaultParser();

        try {
            final CommandLine command = commandLine.parse(COMMAND_LINE_OPTIONS, args);

            final String eBirdFilename = command.getOptionValue(EBIRD_FILE_OPTION);

            DbxRequestConfig config = DbxRequestConfig.newBuilder("dropbox/urban_ebird").build();
            DbxClientV2 client = new DbxClientV2(config, command.getOptionValue(DROPBOX_ACCESS_TOKEN));

            final Optional<Metadata> ebirdFile = client.files().listFolder("").getEntries().stream().filter(
                    metadata -> metadata.getName().equalsIgnoreCase(eBirdFilename)
            ).findFirst();

            if (!ebirdFile.isPresent()) {
                LOGGER.error("Could not find ebird file in Dropbox App folder with name '{0}'. Found:", eBirdFilename);
                client.files().listFolder("").getEntries().forEach(metadata -> LOGGER.error("- " + metadata.getName()));
            } else {
                LOGGER.info("Reading file: {0}", ebirdFile.get().getPathLower());
                final Importer importer = new Importer(new DropboxInputStream(client, ebirdFile.get().getPathLower(), DROPBOX_START_READING, DROPBOX_READ_CHUNK_SIZE));
                importer.importUrbanHotspots();
            }
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "ant", COMMAND_LINE_OPTIONS);
        }

        System.exit(0);
    }
}

