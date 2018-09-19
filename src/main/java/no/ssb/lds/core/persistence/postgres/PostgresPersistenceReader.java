package no.ssb.lds.core.persistence.postgres;

import no.ssb.lds.api.persistence.OutgoingLink;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

class PostgresPersistenceReader {

    private final VertexRepository vertexRepository;
    private final EdgeRepository edgeRepository;

    PostgresPersistenceReader(Connection connection) {
        vertexRepository = new VertexRepository(connection);
        edgeRepository = new EdgeRepository(connection);
    }

    JSONObject getEntity(String namespace, String entity, String id) {
        Vertex vertex = vertexRepository.find(new VertexPrimaryKey(namespace, entity, id), true);
        return (vertex == null || vertex.data == null ? null : new JSONObject(vertex.data));
    }

    JSONArray getEntities(String namespace, String entity) {
        List<String> verticesData = vertexRepository.findAll(new VertexPrimaryKey(namespace, entity, null));
        JSONArray result = new JSONArray();
        for (String data : verticesData) {
            result.put(new JSONObject(data));
        }
        return result;
    }

    Collection<OutgoingLink> getOutgoingEntityLinks(String namespace, String entity, String id) {
        List<OutgoingLink> edges = new ArrayList<>();
        List<Edge> result = edgeRepository.findOutgoingEdgesForVertex(new VertexPrimaryKey(namespace, entity, id));
        for (Edge edge : result) {
            edges.add(new OutgoingLink(null, edge.vertexFromRelationshipURI, edge.vertexFromLabel, namespace, entity, id, edge.vertexToEntity, edge.vertexToId));
        }
        return edges;
    }
}
