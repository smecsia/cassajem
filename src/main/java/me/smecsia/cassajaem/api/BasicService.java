package me.smecsia.cassajaem.api;

import me.smecsia.cassajaem.api.CassajaemException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

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
        List<String> stackTrace = new ArrayList<String>();
        stackTrace.add(e.getClass().getName() + ": " + e.getMessage());
        for (StackTraceElement stE : e.getStackTrace()) {
            String lineNumStr = (stE.getLineNumber() >= 0) ? ":" + stE.getLineNumber() : "";
            stackTrace.add("\t at " + stE.getClassName() + ".<" + stE.getMethodName() + ">(" + stE.getFileName() +
                    lineNumStr + ")");
        }
        getLogger().error(StringUtils.join(stackTrace, "\r\n"));
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
     * Write an exception into the logger and throw the CassajaemRuntimeException next
     *
     * @param msg message
     */
    public void logAndThrow(String msg) {
        getLogger().error(msg);
        throw new CassajaemException(msg);
    }

}
