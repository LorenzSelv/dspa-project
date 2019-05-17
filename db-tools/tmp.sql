SELECT t.person_id2, COUNT(*)
FROM person_knows_person as t
WHERE t.person_id2 NOT IN
(SELECT person_id2 as p2
FROM person_knows_person
WHERE person_id1 = 800)
GROUP BY t.person_id2;

SELECT t2.person_id, COUNT(*) AS NumCommonOrg
FROM person_studyAt_organisation AS t1, person_studyAt_organisation AS t2
WHERE t2.organisation_id = t1.organisation_id
AND t1.person_id = 38
AND t2.person_id <> 38
GROUP BY t2.person_id;

SELECT t2.person_id, COUNT(*) AS NumCommonOrg
FROM person_workAt_organisation AS t1, person_workAt_organisation AS t2
WHERE t2.organisation_id = t1.organisation_id
AND t1.person_id = 38
AND t2.person_id <> 38
GROUP BY t2.person_id;

SELECT ff.person_id3, COUNT(*) as count
FROM person_knows_person AS f,
    (SELECT person_id1 AS person_id2, person_id2 AS person_id3
     FROM person_knows_person
     WHERE person_id1 != 38 AND person_id2 != 38) ff
WHERE f.person_id1 = 38 AND f.person_id2 = ff.person_id2
GROUP BY (f.person_id1, ff.person_id3)
ORDER BY count DESC;

SELECT person_id2
FROM person_knows_person
WHERE person_id1 = 38;

SELECT person_id, COUNT(DISTINCT(forum_id)) as c
FROM forum_has_member
-- WHERE person_id = 20;
GROUP BY person_id
ORDER BY c DESC;
