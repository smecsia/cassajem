package me.smecsia.cassajem.api;

/**
 * Exception related to metadata reading/writing
 * User: smecsia
 * Date: 16.02.12
 * Time: 15:49
 */
public class CassajemMetaException extends CassajemException {
    public CassajemMetaException(String msg) {
        super(msg);
    }

    public CassajemMetaException(Exception e) {
        super(e);
    }
}
