package ebird2postgres.log;

import org.apache.log4j.PropertyConfigurator;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class LoggerFactory {

    private static final Executor EXECUTOR = Executors.newSingleThreadExecutor();

    static {
        PropertyConfigurator.configure("./log4j.properties");
    }

    public static Logger getLogger(final Class<?> clazz) {
        return new Logger(clazz, EXECUTOR);
    }
}
