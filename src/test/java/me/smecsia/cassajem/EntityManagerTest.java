package me.smecsia.cassajem;

import me.smecsia.cassajem.api.AbstractEntity;
import me.smecsia.cassajem.meta.annotations.Column;
import me.smecsia.cassajem.meta.annotations.ColumnFamily;
import me.smecsia.cassajem.meta.annotations.Id;
import org.testng.annotations.Test;

import java.util.List;
import java.util.UUID;

import static me.smecsia.cassajem.util.ConditionsUtil.eq;
import static me.smecsia.cassajem.util.UUIDUtil.timeUUID;
import static org.testng.Assert.*;

public class EntityManagerTest extends CassajemTest {

    @ColumnFamily(name = "users", compositeColumnTypes = {String.class, UUID.class})
    public static class User extends AbstractEntity {
        @Id(persist = false)
        public UUID id = timeUUID();

        @Column
        public String name = "testName";
    }

    @ColumnFamily(name = "projects")
    public static class Project extends AbstractEntity {
        @Id
        public UUID id = timeUUID();

        @Column
        public String name;

        @Column
        public float age;
    }

    @Test
    public void testEntityManager() {
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

    @Test
    public void testDelete() {
        EntityManager<User> em = getService().createEntityManagerFactory().createEntityManager(User.class);
        User user = new User();
        em.save(user);

        assertNotNull(em.find(user.id));
        em.remove(user.id);
        assertNull(em.find(user.id));
    }


    @Test
    public void testPartialUpdateAndSecondaryIndex() {
        EntityManager<Project> em = getService().createEntityManagerFactory().createEntityManager(Project.class);

        // test full save
        Project project = new Project();
        project.name = "testProject";
        project.age = 10;
        em.save(project);

        // test partial save
        project.age = 20;
        em.save(project, "age");

        Project loadedProject = em.find(project.id);
        assertEquals(loadedProject.age, project.age);

        // test secondary indexes
        List<Project> foundList = em.filter(eq("name", project.name));
        assertTrue(!foundList.isEmpty());

        assertEquals(foundList.get(0).id, project.id);
        assertEquals(foundList.get(0).age, project.age);
    }


}
