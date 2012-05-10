## Cassandra Just Entity Mapping

First you need to add the following repository and dependency to your pom.xml:
```xml
    <!-- ... -->
    <dependencies>
        <dependency>
            <groupId>me.smecsia.cassajem</groupId>
            <artifactId>cassajem-core</artifactId>
            <version>0.1-SNAPSHOT</version>
        </dependency>
    </dependencies>
    <!-- ... -->
    <repositories>
        <repository>
            <id>smecsia.me</id>
            <name>smecsia repository</name>
            <url>http://maven.smecsia.me/</url>
        </repository>
    </repositories>
    <!-- ... -->
```
And now you can use all the features of Cassajem:
```java
@ColumnFamily(name = "users")
public class User implements BasicEntity {
    @Id
    private String userName;

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }
}
```
```java
public class Main {
    public static void main(String[] args) throws IOException, TTransportException {

        EmbeddedCassandraService ecs = new EmbeddedCassandraService();
        ecs.init();
        EntityManager<User> em = ecs.createEntityManagerFactory().createEntityManager(User.class);

        User user = new User();
        user.setUserName("john");

        em.save(user);

        User loadedUser = em.find(user.getUserName());

        assert user.getUserName() == loadedUser.getUserName();

        System.exit(0);
    }
}
```
