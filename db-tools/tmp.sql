--

SELECT t1.person_id, COUNT(*)
FROM person_studyAt_organisation AS t1, person_studyAt_organisation AS t2
WHERE t2.organisation_id = t1.organisation_id
-- AND t1.person_id NOT IN
--     (SELECT person_id2
--      FROM person_knows_person
--      WHERE person_id1 = 800)
GROUP BY t1.person_id;
