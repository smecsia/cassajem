package me.smecsia.cassajaem;

import me.smecsia.cassajaem.api.AbstractEntity;
import me.smecsia.cassajaem.meta.annotations.ColumnFamily;
import me.smecsia.cassajaem.meta.annotations.Id;
import me.smecsia.cassajaem.service.EmbeddedCassandraService;
import me.smecsia.cassajaem.service.EntityManagerFactory;
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
