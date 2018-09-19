package no.ssb.lds.core.persistence.postgres;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

class Edge extends EdgePrimaryKey {

    final String vertexFromLabel;
    final VertexPrimaryKey vertexFrom;

    Edge(String vertexToNamespace, String vertexToEntity, String vertexToId, String vertexFromRelationshipURI, String vertexFromLabel, VertexPrimaryKey vertexFrom) {
        super(vertexToNamespace, vertexToEntity, vertexToId, vertexFromRelationshipURI);
        this.vertexFromLabel = vertexFromLabel;
        this.vertexFrom = vertexFrom;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Edge)) return false;
        if (!super.equals(o)) return false;
        Edge edge = (Edge) o;
        return Objects.equals(vertexFromLabel, edge.vertexFromLabel) &&
                Objects.equals(vertexFrom, edge.vertexFrom);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), vertexFromLabel, vertexFrom);
    }

    @Override
    public String toString() {
        return "Edge{" +
                "vertexFromLabel='" + vertexFromLabel + '\'' +
                ", vertexFrom=" + vertexFrom +
                ", vertexToNamespace='" + vertexToNamespace + '\'' +
                ", vertexToEntity='" + vertexToEntity + '\'' +
                ", vertexToId='" + vertexToId + '\'' +
                ", vertexFromRelationshipURI='" + vertexFromRelationshipURI + '\'' +
                '}';
    }

    static Edge instanceOf(ResultSet result) throws SQLException {
        String vertexToNamespace = result.getString(1);
        String vertexToEntity = result.getString(2);
        String vertexToId = result.getString(3);
        String vertexToRelationshipURI = result.getString(4);
        String vertexToRelationshipLabel = result.getString(5);

        String vertexFromNamespace = result.getString(6);
        String vertexFromEntity = result.getString(7);
        String vertexFromId = result.getString(8);

        return new Edge(
                vertexToNamespace,
                vertexToEntity,
                vertexToId,
                vertexToRelationshipURI,
                vertexToRelationshipLabel,
                new VertexPrimaryKey(vertexFromNamespace, vertexFromEntity, vertexFromId));

    }
}
