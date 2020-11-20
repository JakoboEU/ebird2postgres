package ebird2postgres;

import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.Metadata;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Optional;

public class Application {

    private final static Logger LOGGER = LoggerFactory.getLogger(Application.class);

    private final static Options COMMAND_LINE_OPTIONS = new Options();

    private final static String EBIRD_FILE_OPTION = "f";
    private final static String DROPBOX_ACCESS_TOKEN = "a";

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
                LOGGER.error("Could not find ebird file in Dropbox App folder with name '"  + eBirdFilename + "'. Found:");
                client.files().listFolder("").getEntries().forEach(metadata -> System.out.println("- " + metadata.getName()));
            } else {
                LOGGER.info("Reading file: " + ebirdFile.get().getPathLower());
                final InputStream eBirdInput = client.files().download(ebirdFile.get().getPathLower()).getInputStream();
                final Importer importer = new Importer(eBirdInput);
                importer.importUrbanHotspots();
            }

        } catch (ParseException e) {
            System.out.println(e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "ant", COMMAND_LINE_OPTIONS);
        }
    }
}
