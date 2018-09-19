package no.ssb.lds.core.persistence.postgres;

import org.postgresql.util.PSQLException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import javax.sql.DataSource;
import java.sql.Connection;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Ignore
public class PostgresRepositoryTest {

    private DataSource ds;

    @BeforeMethod
    public void before() {
        if (ds == null) {
            ds = PostgresDbInitializer.openDataSource(
                    "localhost",
                    "5432",
                    "lds",
                    "lds",
                    "lds"
            );
        }
    }

    @AfterMethod
    public void after() {
    }

    @Test
    public void testDbConnection() {
        assertNotNull(ds);
    }

    @Test
    public void testVertices() throws Exception {
        Vertex vertexTo = new Vertex("data", "contact", "101", true, "{\"foo\": \"bar\"}");
        Vertex vertexFrom = new Vertex("data", "provisionagreement", "100", true, "{\"foo\": \"bar\"}");

        Connection conn = ds.getConnection();
        conn.beginRequest();

        VertexRepository vertexToRepository = new VertexRepository(conn);
        boolean ok = vertexToRepository.createOrUpdate(vertexTo);
        assertTrue(ok);
        ok = vertexToRepository.createOrUpdate(vertexTo);
        assertTrue(ok);

        ok = vertexToRepository.createOrUpdate(vertexFrom);
        assertTrue(ok);

        conn.commit();
        conn.endRequest();
    }

    @Test
    public void testEdge() throws Exception {
        Connection conn = ds.getConnection();
        VertexRepository vertexToRepository = new VertexRepository(conn);
        EdgeRepository edgeToRepository = new EdgeRepository(conn);

        conn.beginRequest();

        Vertex vertexTo = vertexToRepository.find(new VertexPrimaryKey("data", "contact", "101"));
        Vertex vertexFrom = vertexToRepository.find(new VertexPrimaryKey("data", "provisionagreement", "100"));

        Edge edge = new Edge("data", "contact", "101", "/data/provisionagreement/100/contacts/contact/101", "FRIEND_HAS_REF_TO", new VertexPrimaryKey("data", "provisionagreement", "100"));

        boolean ok = edgeToRepository.createOrUpdate(edge);
        assertTrue(ok);
        ok = edgeToRepository.createOrUpdate(edge);
        assertTrue(ok);

        conn.commit();
        conn.endRequest();
    }

    @Test(expectedExceptions = PSQLException.class)
    public void testVertexToConstrainViolationOnDelete() throws Exception {
        Connection conn = ds.getConnection();
        VertexRepository vertexToRepository = new VertexRepository(conn);

        conn.beginRequest();

        assertTrue(vertexToRepository.delete(new VertexPrimaryKey("data", "contact", "101")));

        conn.commit();
        conn.endRequest();
    }

    @Test(expectedExceptions = PSQLException.class)
    public void testVertexFromConstrainViolationOnDelete() throws Exception {
        Connection conn = ds.getConnection();
        VertexRepository vertexToRepository = new VertexRepository(conn);

        conn.beginRequest();

        assertTrue(vertexToRepository.delete(new VertexPrimaryKey("data", "provisionagreement", "100")));

        conn.commit();
        conn.endRequest();
    }

    @Test
    public void testDeleteEdge() throws Exception {
        Connection conn = ds.getConnection();
        VertexRepository vertexToRepository = new VertexRepository(conn);
        EdgeRepository edgeToRepository = new EdgeRepository(conn);

        conn.beginRequest();

        assertTrue(edgeToRepository.delete(new EdgePrimaryKey("data", "contact", "101", "/data/provisionagreement/100/contacts/contact/101")));
        assertTrue(vertexToRepository.delete(new VertexPrimaryKey("data", "contact", "101")));
        assertTrue(vertexToRepository.delete(new VertexPrimaryKey("data", "provisionagreement", "100")));

        conn.commit();
        conn.endRequest();
    }
}
