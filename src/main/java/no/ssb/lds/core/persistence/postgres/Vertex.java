package no.ssb.lds.core.persistence.postgres;

import org.postgresql.util.PGobject;

import java.sql.ResultSet;
import java.sql.SQLException;

class Vertex extends VertexPrimaryKey {

    final boolean valid;
    final String uri;
    final String data;

    Vertex(String namespace, String entity, String id, boolean valid, String data) {
        super(namespace, entity, id);
        this.valid = valid;
        this.uri = String.format("/%s/%s/%s", namespace, entity, id);
        this.data = data;
    }

    static Vertex instanceOf(ResultSet result) throws SQLException {
        String namespace = result.getString(1);
        String entity = result.getString(2);
        String id = result.getString(3);
        Boolean valid = result.getBoolean(4);
        PGobject data = (PGobject) result.getObject(6);
        return new Vertex(namespace, entity, id, valid, (data != null ? data.getValue() : null));
    }
}
