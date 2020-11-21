package ebird2postgres.log;

import org.apache.commons.logging.LogFactory;

import java.text.MessageFormat;
import java.util.concurrent.Executor;

public class Logger {

    private final Class<?> clazz;
    private final Executor executor;

    public Logger(Class<?> clazz, final Executor executor) {
        this.clazz = clazz;
        this.executor = executor;
    }

    public void trace(String message, Object... parameters) {
        executor.execute(() -> LogFactory.getLog(clazz).trace(MessageFormat.format(message, parameters)));
    }

    public void debug(String message, Object... parameters) {
        executor.execute(() -> LogFactory.getLog(clazz).debug(MessageFormat.format(message, parameters)));
    }

    public void info(String message, Object... parameters) {
        executor.execute(() -> LogFactory.getLog(clazz).info(MessageFormat.format(message, parameters)));
        executor.execute(() -> System.out.println(MessageFormat.format(message, parameters)));
    }

    public void error(String message, Object... parameters) {
        executor.execute(() -> LogFactory.getLog(clazz).error(MessageFormat.format(message, parameters)));
        System.err.println(MessageFormat.format(message, parameters));
    }

    public void error(String message, Throwable error) {
        executor.execute(() -> LogFactory.getLog(clazz).error(message, error));
        System.err.println(message);
        error.printStackTrace();
    }
}
