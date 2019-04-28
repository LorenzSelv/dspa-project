CREATE TABLE IF NOT EXISTS person_knows_person (
person1 integer NOT NULL,
person2 integer NOT NULL,
PRIMARY KEY (person1, person2)
);

DELETE FROM person_knows_person;

\set tables `pwd`'/dataset/1k-users-sorted/tables/'

\set t1 :tables'person_knows_person.csv'
COPY person_knows_person FROM :'t1' DELIMITERS '|' CSV HEADER;
