import org.slf4j.LoggerFactory;

public interface Logger {
    org.slf4j.Logger logger = LoggerFactory.getLogger(Logger.class.getSimpleName());
}
