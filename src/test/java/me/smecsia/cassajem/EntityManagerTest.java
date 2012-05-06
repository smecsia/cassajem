package me.smecsia.cassajem;

import me.smecsia.cassajem.api.AbstractEntity;
import me.smecsia.cassajem.meta.annotations.ColumnFamily;
import me.smecsia.cassajem.meta.annotations.Id;
import me.smecsia.cassajem.service.EmbeddedCassandraService;
import org.apache.thrift.transport.TTransportException;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.UUID;

import static me.smecsia.cassajem.util.UUIDUtil.timeUUID;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class EntityManagerTest {
    
    @ColumnFamily(name = "testEntities")
    public static class TestEntity extends AbstractEntity {
        @Id
        public UUID id = timeUUID();
    }
    
    @Test
    public void testEntityManager() throws IOException, TTransportException {
        EmbeddedCassandraService ecs = new EmbeddedCassandraService("/cassandra-test.yaml");
        ecs.init();
        EntityManagerFactory emf = ecs.createEntityManagerFactory();
        EntityManager<TestEntity> em = emf.createEntityManager(TestEntity.class);
        
        TestEntity entity = new TestEntity();
        
        em.save(entity);
        
        TestEntity loaded = em.find(entity.id);

        assertNotNull(loaded);
        assertEquals(loaded.id, entity.id);
        
    }
}
