package no.ssb.lds.core.persistence.postgres;

import no.ssb.lds.api.persistence.OutgoingLink;
import org.json.JSONObject;

import java.sql.Connection;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

class PostgresPersistenceWriter {

    private final VertexRepository vertexRepository;
    private final EdgeRepository edgeRepository;

    PostgresPersistenceWriter(Connection connection) {
        vertexRepository = new VertexRepository(connection);
        edgeRepository = new EdgeRepository(connection);
    }

    boolean createOrOverwriteEntity(String namespace, String entity, String id, JSONObject jsonObject) {
        Vertex vertexFrom = new Vertex(namespace, entity, id, true, jsonObject.toString());
        return vertexRepository.createOrUpdate(vertexFrom);
    }

    boolean deleteEntity(String namespace, String entity, String id) {
        List<Edge> incomingEdges = edgeRepository.findIncomingEdgesForVertex(new VertexPrimaryKey(namespace, entity, id));
        for (Edge edge : incomingEdges) {
            edgeRepository.delete(edge);
        }

        List<Edge> outgoingEdges = edgeRepository.findOutgoingEdgesForVertex(new VertexPrimaryKey(namespace, entity, id));
        for (Edge edge : outgoingEdges) {
            edgeRepository.delete(edge);
        }

        return vertexRepository.delete(new VertexPrimaryKey(namespace, entity, id));
    }

    boolean mergeOutgoingEntityLinks(Collection<OutgoingLink> wantedLinks, Collection<OutgoingLink> existingOutgoingLinks) {
        Set<OutgoingLink> linksToBeCreated = new LinkedHashSet<>(wantedLinks);
        linksToBeCreated.removeAll(existingOutgoingLinks);
        Set<OutgoingLink> linksToBeRemoved = new LinkedHashSet<>(existingOutgoingLinks);
        linksToBeRemoved.removeAll(wantedLinks);
        linksToBeCreated.forEach(this::createOutgoingEntityLink);
        linksToBeRemoved.forEach(this::deleteOutgoingEntityLink);
        return !linksToBeCreated.isEmpty() || !linksToBeRemoved.isEmpty();
    }

    private boolean createOutgoingEntityLink(OutgoingLink outgoingLink) {
        if (vertexRepository.find(new VertexPrimaryKey(outgoingLink.namespace, outgoingLink.edgeEntity, outgoingLink.edgeId), true) == null) {
            Vertex voidVertexTo = new Vertex(outgoingLink.namespace, outgoingLink.edgeEntity, outgoingLink.edgeId, false, null);
            vertexRepository.createOrUpdate(voidVertexTo);
        }

        Edge value = new Edge(outgoingLink.namespace, outgoingLink.edgeEntity, outgoingLink.edgeId, outgoingLink.relationshipURI, outgoingLink.relationshipName,
                new VertexPrimaryKey(outgoingLink.namespace, outgoingLink.entity, outgoingLink.id));

        return edgeRepository.createOrUpdate(value);
    }

    private boolean deleteOutgoingEntityLink(OutgoingLink outgoingLink) {
        return edgeRepository.delete(new EdgePrimaryKey(outgoingLink.namespace, outgoingLink.edgeEntity, outgoingLink.edgeId, outgoingLink.relationshipURI));
    }
}
