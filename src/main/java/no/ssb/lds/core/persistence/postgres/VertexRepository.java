package no.ssb.lds.core.persistence.postgres;

import no.ssb.lds.api.persistence.PersistenceException;
import org.postgresql.util.PGobject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

class VertexRepository extends AbstractRepository<Vertex, VertexPrimaryKey> {

    VertexRepository(Connection conn) {
        super(conn);
    }

    @Override
    boolean createOrUpdate(Vertex value) throws PersistenceException {
        if (find(value) == null) {
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO vertex (namespace, entity, id, valid, uri, data) VALUES (?, ?, ?, ?, ?, ?)")) {
                ps.setString(1, value.namespace);
                ps.setString(2, value.entity);
                ps.setString(3, value.id);
                ps.setBoolean(4, value.valid);
                ps.setString(5, value.uri);
                PGobject jsonObject = new PGobject();
                jsonObject.setType("json");
                jsonObject.setValue(value.data);
                ps.setObject(6, jsonObject);
                return ps.executeUpdate() > 0;
            } catch (SQLException e) {
                throw new PersistenceException(e);
            }
        } else {
            try (PreparedStatement ps = conn.prepareStatement("UPDATE vertex SET valid = ?, data = ? WHERE namespace = ? AND entity = ? AND id = ?")) {
                ps.setBoolean(1, value.valid);
                PGobject jsonObject = new PGobject();
                jsonObject.setType("json");
                jsonObject.setValue(value.data);
                ps.setObject(2, jsonObject);
                ps.setString(3, value.namespace);
                ps.setString(4, value.entity);
                ps.setString(5, value.id);
                return (ps.executeUpdate() <= 0); // return false if affected update rows > 0, otherwise true (something wrong occured)
            } catch (SQLException e) {
                throw new PersistenceException(e);
            }
        }
    }

    @Override
    boolean delete(VertexPrimaryKey primaryKey) throws PersistenceException {
        try (PreparedStatement ps = conn.prepareStatement("DELETE FROM vertex WHERE namespace = ? AND entity = ? AND id = ?")) {
            ps.setString(1, primaryKey.namespace);
            ps.setString(2, primaryKey.entity);
            ps.setString(3, primaryKey.id);
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    Vertex find(VertexPrimaryKey primaryKey) throws PersistenceException {
        return find(primaryKey, null);
    }

    Vertex find(VertexPrimaryKey primaryKey, Boolean valid) throws PersistenceException {
        String sql = String.format("SELECT * FROM vertex WHERE namespace = ? AND entity = ? AND id = ? %s", (valid == null ? "" : " AND valid = " + valid.booleanValue()));
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, primaryKey.namespace);
            ps.setString(2, primaryKey.entity);
            ps.setString(3, primaryKey.id);
            try (ResultSet result = ps.executeQuery()) {
                while (result.next()) {
                    return Vertex.instanceOf(result);
                }
            }
        } catch (SQLException e) {
            throw new PersistenceException(e);
        }
        return null;
    }

    List<String> findAll(VertexPrimaryKey primaryKey) throws PersistenceException {
        return findAll(primaryKey, true);
    }

    List<String> findAll(VertexPrimaryKey primaryKey, Boolean valid) throws PersistenceException {
        String sql = String.format("SELECT data FROM vertex WHERE namespace = ? AND entity = ? %s LIMIT 250", (valid == null ? "" : " AND valid = " + valid.booleanValue()));
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, primaryKey.namespace);
            ps.setString(2, primaryKey.entity);
            List<String> list = new ArrayList<>();
            try (ResultSet result = ps.executeQuery()) {
                while (result.next()) {
                    list.add(result.getString(1));
                }
                return list;
            }
        } catch (SQLException e) {
            throw new PersistenceException(e);
        }
    }

}
