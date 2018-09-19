package no.ssb.lds.core.persistence.postgres;

class VertexPrimaryKey {

    final String namespace;
    final String entity;
    final String id;

    VertexPrimaryKey(String namespace, String entity, String id) {
        this.namespace = namespace;
        this.entity = entity;
        this.id = id;
    }
}
