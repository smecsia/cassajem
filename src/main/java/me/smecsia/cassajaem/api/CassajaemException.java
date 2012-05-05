package me.smecsia.cassajaem.api;

/**
 * Basic exception of the database engine
 * User: isadykov
 * Date: 16.02.12
 * Time: 15:49
 */
public class CassajaemException extends RuntimeException {
    public CassajaemException(String msg) {
        super(msg);
    }
}
