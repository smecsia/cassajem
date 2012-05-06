package me.smecsia.cassajem.meta;

import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.smecsia.cassajem.api.AbstractEntity;
import me.smecsia.cassajem.api.BasicEntity;
import me.smecsia.cassajem.api.CompositeKey;
import me.smecsia.cassajem.meta.annotations.*;
import me.smecsia.cassajem.util.TypesUtil;
import org.apache.cassandra.db.marshal.BytesType;
import org.testng.annotations.Test;

import java.util.*;

import static org.testng.Assert.*;

/**
 * Created by IntelliJ IDEA.
 * User: isadykov
 * Date: 27.01.12
 * Time: 18:43
 */
public class MetadataTest {

    @ColumnFamily(name = "simple_entity", compositeColumnTypes = {UUID.class, String.class})
    public static class SimpleEntity extends AbstractEntity {
        @Id(keyValidationClass = BytesType.class)
        private String id;

        @Column
        private UUID uuidColumn;

        @Column
        private String someColumn;

        @Column(name = "someLongLongColumn")
        private long someLongColumn;

        @DynamicColumnStorage
        private Map<String, String> dynamicStorage = new HashMap<String, String>();

        @CompositeColumnArray
        private Map<CompositeKey, Composite> compositeArray = new HashMap<CompositeKey, Composite>();

        public void setId(String id) {
            this.id = id;
        }

        public void setSomeColumn(String someColumn) {
            this.someColumn = someColumn;
        }

        public Map<String, String> getDynamicStorage() {
            return dynamicStorage;
        }
    }

    @Test
    public void testMetadata() {
        Metadata<SimpleEntity> metadata = new Metadata<SimpleEntity>(SimpleEntity.class);

        assertEquals(metadata.getColumns().size(), 3);
        assertEquals(metadata.getIdColumn().getFieldName(), "id");
        assertEquals(metadata.getColumns().get("someColumn").getFieldName(), "someColumn");
        assertTrue(metadata.getColumns().get("uuidColumn").getValueSerializer() instanceof UUIDSerializer);

        assertTrue(metadata.getAllColumns().containsKey("id"));
        assertFalse(metadata.getAllColumns("someColumn").containsKey("id"));

        assertEquals(metadata.getEntityInfo().getColumnType(), ColumnType.STANDARD);
        assertNotNull(metadata.getDynamicColumnsStorage());
        assertEquals(metadata.getDynamicColumnsStorage().getFieldName(), "dynamicStorage");

        assertNotNull(metadata.getCompositeArrays());
        assertEquals(metadata.getEntityInfo().compositeColumnTypes()[0], UUID.class);

        assertEquals(metadata.getCompositeArrays().get("compositeArray").getValueSerializer(),
                TypesUtil.compositeSerializer);
        assertTrue(metadata.getCompositeArrays("notExistCompositeArray").size() < 1);
        assertTrue(metadata.getCompositeArrays("compositeArray").size() > 0);

        assertEquals(metadata.getKeyValidationClass(), BytesType.class);

        assertEquals(metadata.getComparatorType(), TypesUtil.compositeComparator);

        assertEquals(metadata.getColumns().get("someLongLongColumn").getValueSerializer(), TypesUtil.longSerializer);

        SimpleEntity testEntity = new SimpleEntity();
        testEntity.setId("testId");
        testEntity.setSomeColumn("someValue");
        testEntity.getDynamicStorage().put("key", "value");

        assertEquals(metadata.getIdColumn().invokeGetter(testEntity), "testId");

        assertEquals(metadata.getColumns().get("someColumn").invokeGetter(testEntity), "someValue");
        assertEquals(((Map) metadata.getDynamicColumnsStorage().invokeGetter(testEntity)).get("key"), "value");

    }

    @ColumnFamily(columnType = ColumnType.SUPER, name = "superColumnFamily")
    public static class SuperColumnFamilyEntity extends AbstractEntity implements BasicEntity {
        @Id
        private UUID id;

        public TestSuperColumn getTestSuperColumn() {
            return testSuperColumn;
        }

        public void setTestSuperColumn(TestSuperColumn testSuperColumn) {
            this.testSuperColumn = testSuperColumn;
        }

        public UUID getId() {
            return id;
        }

        public void setId(UUID id) {
            this.id = id;
        }

        public Map<String, String> getTestColumnArray() {
            return testColumnArray;
        }

        public void setTestColumnArray(Map<String, String> testColumnArray) {
            this.testColumnArray = testColumnArray;
        }

        public List<TestSuperColumn> getTestSuperColumnArray() {
            return testSuperColumnArray;
        }

        public void setTestSuperColumnArray(List<TestSuperColumn> testSuperColumnArray) {
            this.testSuperColumnArray = testSuperColumnArray;
        }

        public Map<String, TestSuperColumn> getDynamicSuperStorage() {
            return dynamicSuperStorage;
        }

        public void setDynamicSuperStorage(Map<String, TestSuperColumn> dynamicSuperStorage) {
            this.dynamicSuperStorage = dynamicSuperStorage;
        }

        public static class TestSuperColumn extends AbstractEntity implements BasicEntity {

            @Column(name = "scColumn")
            private String scColumn;

            public String getScColumn() {
                return scColumn;
            }

            public void setScColumn(String scColumn) {
                this.scColumn = scColumn;
            }
        }

        @SuperColumn(name = "testSuperColumn")
        private TestSuperColumn testSuperColumn;

        @SuperColumnArray(name = "testSuperColumnArray")
        private List<TestSuperColumn> testSuperColumnArray = new ArrayList<TestSuperColumn>();

        @ColumnArray(name = "testColumnArray")
        private Map<String, String> testColumnArray;

        @DynamicSuperColumnStorage
        private Map<String, TestSuperColumn> dynamicSuperStorage = new HashMap<String, TestSuperColumn>();

    }

    @Test
    public void testSuperColumnFamily() {
        Metadata<SuperColumnFamilyEntity> metadata = new Metadata<SuperColumnFamilyEntity>(
                SuperColumnFamilyEntity.class);

        assertEquals(metadata.getEntityInfo().getColumnType(), ColumnType.SUPER);

        assertEquals(metadata.getColumnArrays().get("testColumnArray").getFieldName(), "testColumnArray");

        assertEquals(metadata.getSuperColumnArrays().get("testSuperColumnArray").getFieldName(), "testSuperColumnArray");

        assertEquals(metadata.getSuperColumnArrays().get("testSuperColumnArray").getColumnMetaData().getColumns()
                .size(), 1);

        assertNotNull(metadata.getDynamicSuperColumnsStorage());
        assertEquals(metadata.getDynamicSuperColumnsStorage().getColumnMetaData().getColumns().size(), 1);
    }

}
