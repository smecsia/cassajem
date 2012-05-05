package me.smecsia.cassajem;

import me.smecsia.cassajem.api.AbstractEntity;
import me.smecsia.cassajem.meta.annotations.ColumnFamily;
import me.smecsia.cassajem.meta.annotations.Id;
import me.smecsia.cassajem.service.EmbeddedCassandraService;
import me.smecsia.cassajem.service.EntityManagerFactory;
import org.testng.annotations.Test;

public class EntityManagerTest {
    
    @ColumnFamily(name = "testEntities")
    public static class TestEntity extends AbstractEntity {
        @Id
        private String id;
    }
    
    @Test
    public void testEntityManager(){

        EmbeddedCassandraService ecs = new EmbeddedCassandraService("/cassandra-test.yaml");


        EntityManagerFactory emf = new EntityManagerFactory();
        
    }
}
