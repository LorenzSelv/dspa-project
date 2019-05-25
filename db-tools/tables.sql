CREATE TABLE IF NOT EXISTS person_knows_person (
person_id1 BIGINT NOT NULL,
person_id2 BIGINT NOT NULL,
PRIMARY KEY (person_id1, person_id2)
);

DELETE FROM person_knows_person;

\set tables `pwd`'/dataset/':db'/tables/'

\set t1 :tables'person_knows_person.csv'
COPY person_knows_person FROM :'t1' DELIMITERS '|' CSV HEADER;


CREATE TABLE IF NOT EXISTS person_studyAt_organisation (
person_id        BIGINT NOT NULL,
organisation_id  BIGINT NOT NULL,
class_year       INTEGER,
PRIMARY KEY (person_id, organisation_id)
);

DELETE FROM person_studyAt_organisation;

\set t1 :tables'person_studyAt_organisation.csv'
COPY person_studyAt_organisation FROM :'t1' DELIMITERS '|' CSV HEADER;


CREATE TABLE IF NOT EXISTS person_workAt_organisation (
person_id       BIGINT NOT NULL,
organisation_id BIGINT NOT NULL,
works_from      INTEGER,
PRIMARY KEY (person_id, organisation_id)
);

DELETE FROM person_workAt_organisation;

\set t1 :tables'person_workAt_organisation.csv'
COPY person_workAt_organisation FROM :'t1' DELIMITERS '|' CSV HEADER;

DROP TABLE forum_has_member;

CREATE TABLE IF NOT EXISTS forum_has_member (
forum_id       BIGINT NOT NULL,
person_id      BIGINT NOT NULL,
join_date      TIMESTAMP,
PRIMARY KEY    (forum_id, person_id)
);

DELETE FROM forum_has_member;

\set t1 :tables'forum_hasMember_person.csv'
COPY forum_has_member FROM :'t1' DELIMITERS '|' CSV HEADER;
