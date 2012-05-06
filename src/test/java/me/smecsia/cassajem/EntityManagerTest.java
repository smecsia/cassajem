package me.smecsia.cassajem;

import me.smecsia.cassajem.api.AbstractEntity;
import me.smecsia.cassajem.meta.annotations.Column;
import me.smecsia.cassajem.meta.annotations.ColumnFamily;
import me.smecsia.cassajem.meta.annotations.Id;
import org.apache.thrift.transport.TTransportException;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.UUID;

import static me.smecsia.cassajem.util.UUIDUtil.timeUUID;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class EntityManagerTest extends CassajemTest{

    @ColumnFamily(name = "users", compositeColumnTypes = {String.class, UUID.class})
    public static class User extends AbstractEntity {
        @Id(persist = false)
        public UUID id = timeUUID();
        
        @Column
        public String name = "testName";
    }

    @Test
    public void testEntityManager() throws IOException, TTransportException {
        EntityManagerFactory emf = getService().createEntityManagerFactory();
        EntityManager<User> em = emf.createEntityManager(User.class);

        User user = new User();
        user.name = "John Smith";
        
        em.save(user);
        
        User loadedUser = em.find(user.id);

        assertNotNull(loadedUser);
        assertEquals(loadedUser.id, user.id);

        assertEquals(loadedUser.name, user.name);
        
    }
}
