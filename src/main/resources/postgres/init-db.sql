DROP TABLE IF EXISTS namespace;

CREATE TABLE namespace
(
  entity  varchar COLLATE "POSIX"     NOT NULL,
  id      varchar COLLATE "POSIX"     NOT NULL,
  version timestamp(3) with time zone NOT NULL,
  path    varchar                     NOT NULL,
  indices integer[]                   NOT NULL,
  type    smallint                    NOT NULL,
  value   bytea NULL,
  PRIMARY KEY (entity, id, version, path, indices)
);

CREATE INDEX namespace_path_value_idx ON namespace(entity, path, value);
