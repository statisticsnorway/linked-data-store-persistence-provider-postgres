DROP TABLE IF EXISTS edge CASCADE;
DROP TABLE IF EXISTS vertex CASCADE;

CREATE TABLE vertex (
  namespace   varchar(255),
  entity      varchar(255),
  id          varchar(255),
  valid       boolean,
  uri         varchar(255) NOT NULL,
  data        json,
  PRIMARY KEY (namespace, entity, id)
);

CREATE TABLE edge (
  vertex_to_namespace             varchar(255) NOT NULL,
  vertex_to_entity                varchar(255) NOT NULL,
  vertex_to_id                    varchar(255) NOT NULL,
  vertex_from_relationship_uri    varchar(255) NOT NULL,
  vertex_from_label               varchar(255) NOT NULL,
  vertex_from_namespace           varchar(255) NOT NULL,
  vertex_from_entity              varchar(255) NOT NULL,
  vertex_from_id                  varchar(255) NOT NULL,
  FOREIGN KEY (vertex_to_namespace, vertex_to_entity, vertex_to_id) REFERENCES vertex (namespace, entity, id),
  FOREIGN KEY (vertex_from_namespace, vertex_from_entity, vertex_from_id) REFERENCES vertex (namespace, entity, id),
  PRIMARY KEY (vertex_to_namespace, vertex_to_entity, vertex_to_id, vertex_from_relationship_uri)
);

