package no.ssb.lds.core.persistence.postgres;

import no.ssb.lds.api.persistence.PersistenceException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

class EdgeRepository extends AbstractRepository<Edge, EdgePrimaryKey> {

    EdgeRepository(Connection conn) {
        super(conn);
    }

    @Override
    boolean createOrUpdate(Edge value) throws PersistenceException {
        if (find(value) == null) {
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO edge " +
                    "(vertex_to_namespace, vertex_to_entity, vertex_to_id, vertex_from_relationship_uri, vertex_from_label, vertex_from_namespace, vertex_from_entity, vertex_from_id) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)")) {
                ps.setString(1, value.vertexToNamespace);
                ps.setString(2, value.vertexToEntity);
                ps.setString(3, value.vertexToId);
                ps.setString(4, value.vertexFromRelationshipURI);
                ps.setString(5, value.vertexFromLabel);
                ps.setString(6, value.vertexFrom.namespace);
                ps.setString(7, value.vertexFrom.entity);
                ps.setString(8, value.vertexFrom.id);
                ps.executeUpdate();
                return true;
            } catch (SQLException e) {
                throw new PersistenceException(e);
            }
        } else {
            // do nothing on update
            return true;
        }
    }

    @Override
    boolean delete(EdgePrimaryKey primaryKey) throws PersistenceException {
        try (PreparedStatement ps = conn.prepareStatement("DELETE FROM edge WHERE vertex_to_namespace = ? AND vertex_to_entity = ? AND vertex_to_id = ? AND vertex_from_relationship_uri = ?")) {
            ps.setString(1, primaryKey.vertexToNamespace);
            ps.setString(2, primaryKey.vertexToEntity);
            ps.setString(3, primaryKey.vertexToId);
            ps.setString(4, primaryKey.vertexFromRelationshipURI);
            ps.executeUpdate();
            return true;
        } catch (SQLException e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    Edge find(EdgePrimaryKey primaryKey) throws PersistenceException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT * FROM edge WHERE vertex_to_namespace = ? AND vertex_to_entity = ? AND vertex_to_id = ? AND vertex_from_relationship_uri = ?")) {
            ps.setString(1, primaryKey.vertexToNamespace);
            ps.setString(2, primaryKey.vertexToEntity);
            ps.setString(3, primaryKey.vertexToId);
            ps.setString(4, primaryKey.vertexFromRelationshipURI);
            try (ResultSet result = ps.executeQuery()) {
                while (result.next()) {
                    return Edge.instanceOf(result);
                }
            }
        } catch (SQLException e) {
            throw new PersistenceException(e);
        }
        return null;
    }

    List<Edge> findIncomingEdgesForVertex(VertexPrimaryKey primaryKey) throws PersistenceException {
        List<Edge> list = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement("SELECT * FROM edge WHERE vertex_to_namespace = ? AND vertex_to_entity = ? AND vertex_to_id = ?")) {
            ps.setString(1, primaryKey.namespace);
            ps.setString(2, primaryKey.entity);
            ps.setString(3, primaryKey.id);
            try (ResultSet result = ps.executeQuery()) {
                while (result.next()) {
                    list.add(Edge.instanceOf(result));
                }
            }
        } catch (SQLException e) {
            throw new PersistenceException(e);
        }
        return list;
    }

    List<Edge> findOutgoingEdgesForVertex(VertexPrimaryKey primaryKey) throws PersistenceException {
        List<Edge> list = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement("SELECT * FROM edge WHERE vertex_from_namespace = ? AND vertex_from_entity = ? AND vertex_from_id = ?")) {
            ps.setString(1, primaryKey.namespace);
            ps.setString(2, primaryKey.entity);
            ps.setString(3, primaryKey.id);
            try (ResultSet result = ps.executeQuery()) {
                while (result.next()) {
                    list.add(Edge.instanceOf(result));
                }
            }
        } catch (SQLException e) {
            throw new PersistenceException(e);
        }
        return list;
    }
}
