package me.smecsia.cassajem.util;

import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class ExceptionUtil {

    public static String getFormattedStacktrace(Exception e){
        return StringUtils.join(getFormattedStacktraceList(e), "\r\n");
    }

    public static List<String> getFormattedStacktraceList(Exception e){
        List<String> stackTrace = new ArrayList<String>();
        stackTrace.add(e.getClass().getName() + ": " + e.getMessage());
        for (StackTraceElement stE : e.getStackTrace()) {
            String lineNumStr = (stE.getLineNumber() >= 0) ? ":" + stE.getLineNumber() : "";
            stackTrace.add("\t at " + stE.getClassName() + ".<" + stE.getMethodName() + ">(" + stE.getFileName() +
                    lineNumStr + ")");
        }
        return stackTrace;
    }
}
