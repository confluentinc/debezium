-- Smart snapshot embedded-engine IT fixture: two small tables with a couple of rows each.
-- The database is created and selected by UniqueDatabase before these statements run.
CREATE TABLE a (
  id INT NOT NULL PRIMARY KEY,
  v  VARCHAR(64)
);
INSERT INTO a VALUES (1, 'a1'), (2, 'a2');

CREATE TABLE b (
  id INT NOT NULL PRIMARY KEY,
  v  VARCHAR(64)
);
INSERT INTO b VALUES (1, 'b1'), (2, 'b2');
