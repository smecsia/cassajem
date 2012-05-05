package me.smecsia.cassajaem.meta;

/**
 * Created by IntelliJ IDEA.
 * User: isadykov
 * Date: 27.01.12
 * Time: 13:53
 */
public class SuperColumnInfo extends ColumnInfo {
    private Metadata columnMetaData;

    public SuperColumnInfo(Metadata metadata) {
        columnMetaData = metadata;
    }

    public Metadata getColumnMetaData() {
        return columnMetaData;
    }

    public void setColumnMetaData(Metadata columnMetaData) {
        this.columnMetaData = columnMetaData;
    }

}
