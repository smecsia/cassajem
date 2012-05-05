package me.smecsia.cassajem.meta;

import me.smecsia.cassajem.meta.annotations.ColumnFamily;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.ddl.ColumnType;

import static me.smecsia.cassajem.util.TypesUtil.serializerByType;

/**
 * Class holding the metadata about the entity class (storage for other metadata info)
 * Created by IntelliJ IDEA.
 * User: isadykov
 * Date: 27.01.12
 * Time: 13:54
 */
public class EntityInfo {
    // column family name
    public String cfName;

    public SuperColumnInfo dataStorageSuperColumn = null;

    public Class[] compositeColumnTypes = {};

    public Serializer[] serializers = {};

    public ColumnType columnType;

    public boolean useTimeForUUIDs = true;

    public ColumnFamily.SavePolicy savePolicy = ColumnFamily.SavePolicy.REWRITE;

    public ColumnType getColumnType() {
        return columnType;
    }

    public void setColumnType(ColumnType columnType) {
        this.columnType = columnType;
    }

    public Class[] compositeColumnTypes() {
        return compositeColumnTypes;
    }

    public Serializer[] compositeSerializers() {
        return serializers;
    }

    public void setCompositeSerializers(Serializer[] serializers) {
        this.serializers = serializers;
    }

    /**
     * Set the types of a composite key (if it is used)
     *
     * @param compositeColumnTypes list of composite column types
     */
    public void setCompositeColumnTypes(Class[] compositeColumnTypes) {
        this.compositeColumnTypes = compositeColumnTypes;
        this.serializers = new Serializer[compositeColumnTypes.length];
        for (int i = 0; i < compositeColumnTypes.length; ++i) {
            this.serializers[i] = serializerByType(compositeColumnTypes[i]);
        }
    }

    /**
     * Returns true if this CF uses the composite keys
     *
     * @return true if composite keys are used
     */
    public boolean usesComposites() {
        return this.compositeColumnTypes.length > 0;
    }

    public SuperColumnInfo getDataStorageSuperColumn() {
        return dataStorageSuperColumn;
    }

    public void setDataStorageSuperColumn(SuperColumnInfo dataStorageSuperColumn) {
        this.dataStorageSuperColumn = dataStorageSuperColumn;
    }

    public String getCfName() {
        return cfName;
    }

    public void setCfName(String cfName) {
        this.cfName = cfName;
    }

    public ColumnFamily.SavePolicy getSavePolicy() {
        return savePolicy;
    }

    public void setSavePolicy(ColumnFamily.SavePolicy savePolicy) {
        this.savePolicy = savePolicy;
    }

    public boolean useTimeForUUIDs() {
        return useTimeForUUIDs;
    }

    public void setUseTimeForUUIDs(boolean useTimeForUUIDs) {
        this.useTimeForUUIDs = useTimeForUUIDs;
    }
}
