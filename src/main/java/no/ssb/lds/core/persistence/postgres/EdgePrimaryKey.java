package no.ssb.lds.core.persistence.postgres;

import java.util.Objects;

class EdgePrimaryKey {

    final String vertexToNamespace;
    final String vertexToEntity;
    final String vertexToId;
    final String vertexFromRelationshipURI;

    EdgePrimaryKey(String vertexToNamespace, String vertexToEntity, String vertexToId, String vertexFromRelationshipURI) {
        this.vertexToNamespace = vertexToNamespace;
        this.vertexToEntity = vertexToEntity;
        this.vertexToId = vertexToId;
        this.vertexFromRelationshipURI = vertexFromRelationshipURI;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EdgePrimaryKey)) return false;
        EdgePrimaryKey that = (EdgePrimaryKey) o;
        return Objects.equals(vertexToNamespace, that.vertexToNamespace) &&
                Objects.equals(vertexToEntity, that.vertexToEntity) &&
                Objects.equals(vertexToId, that.vertexToId) &&
                Objects.equals(vertexFromRelationshipURI, that.vertexFromRelationshipURI);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vertexToNamespace, vertexToEntity, vertexToId, vertexFromRelationshipURI);
    }

    @Override
    public String toString() {
        return "EdgePrimaryKey{" +
                "vertexToNamespace='" + vertexToNamespace + '\'' +
                ", vertexToEntity='" + vertexToEntity + '\'' +
                ", vertexToId='" + vertexToId + '\'' +
                ", vertexFromRelationshipURI='" + vertexFromRelationshipURI + '\'' +
                '}';
    }
}
