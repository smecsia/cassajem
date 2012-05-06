package me.smecsia.cassajem.dao;

import me.smecsia.cassajem.CassajemTest;
import me.smecsia.cassajem.EntityManagerFactory;
import me.smecsia.cassajem.api.AbstractEntity;
import me.smecsia.cassajem.meta.annotations.ColumnFamily;
import me.smecsia.cassajem.meta.annotations.EntityContext;
import me.smecsia.cassajem.meta.annotations.Id;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class DAOFactoryTest extends CassajemTest {

    @ColumnFamily(name = "sessions")
    public static class Session extends AbstractEntity {
        @Id
        public String id;
    }

    @EntityContext(entityClass = Session.class)
    public static class SessionDAO extends AbstractDAO<Session> {
        public SessionDAO(EntityManagerFactory entityManagerFactory, DAOFactory daoFactory) {
            super(entityManagerFactory, daoFactory);
        }
    }

    @Test
    public void testCustomDAO() {
        DAOFactory daoFactory = new DAOFactory(getService().createEntityManagerFactory(), "me.smecsia.cassajem.dao.DAOFactoryTest$");
        assertTrue(daoFactory.getDAO(Session.class) instanceof SessionDAO);
    }

    @Test
    public void testBasicDAO() {
        DAOFactory daoFactory = new DAOFactory(getService().createEntityManagerFactory());

        DAO<Session> sessionDAO = daoFactory.getDAO(Session.class);

        Session session = new Session();
        session.id = "someId";

        sessionDAO.save(session);


        Session loadedSession = sessionDAO.byId(session.id);

        assertNotNull(loadedSession);
        assertEquals(loadedSession.id, session.id);
    }
}
