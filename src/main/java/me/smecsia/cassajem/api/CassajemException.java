package me.smecsia.cassajem.api;

/**
 * Basic exception of the database engine
 * User: smecsia
 * Date: 16.02.12
 * Time: 15:49
 */
public class CassajemException extends RuntimeException {
    public CassajemException(String msg) {
        super(msg);
    }

    public CassajemException(Exception e){
        super(e);
    }
}
