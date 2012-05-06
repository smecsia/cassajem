package me.smecsia.cassajem.meta;

import me.smecsia.cassajem.api.AbstractEntity;
import me.smecsia.cassajem.meta.annotations.ColumnFamily;
import me.smecsia.cassajem.meta.annotations.Id;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * Created by IntelliJ IDEA.
 * User: isadykov
 * Date: 22.02.12
 * Time: 15:03
 */
public class MetaCacheTest {

    @ColumnFamily(name = "entityClass")
    public static class EntityClass extends AbstractEntity {
        @Id(persist = false)
        private int id;

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }
    }

    @Test
    public void testMetaCache() {
        // test we get the same instance both times
        assertEquals(
                MetaCache.instance().forClass(EntityClass.class),
                MetaCache.instance().forClass(EntityClass.class)
        );
    }
}
