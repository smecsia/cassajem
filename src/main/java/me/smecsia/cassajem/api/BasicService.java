package me.smecsia.cassajem.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static me.smecsia.cassajem.util.ExceptionUtil.getFormattedStacktrace;

/**
 * Basic class for all services.
 * Just autowires some useful things such as logger and provides the
 * ability to logAndThrow some exceptions
 * User: isadykov
 * Date: 14.03.12
 * Time: 17:31
 */
public abstract class BasicService {

    
    protected Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * Log and throw the exception next
     *
     * @param e exception
     */
    public void logAndThrow(Exception e) {
        getLogger().error(getFormattedStacktrace(e));
        logAndThrow(e.getMessage());
    }

    /**
     * Returns current logger. Override this to use another logger
     *
     * @return current logger
     */
    public Logger getLogger() {
        return logger;
    }

    /**
     * Write an exception into the logger and throw the CassajemException next
     *
     * @param msg message
     * @param e exception
     */
    public void logAndThrow(String msg, Exception e) {
        getLogger().error(msg);
        getLogger().error(getFormattedStacktrace(e));
        throw new CassajemException(e);
    }

    /**
     * Write an exception into the logger and throw the CassajemException next
     *
     * @param msg message
     */
    public void logAndThrow(String msg) {
        getLogger().error(msg);
        throw new CassajemException(msg);
    }

}
