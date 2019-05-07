const studyAt: &'static str = "SELECT t2.person_id, COUNT(*) AS NumCommonOrg
    FROM person_studyAt_organisation AS t1, person_studyAt_organisation AS t2
    WHERE t2.organisation_id = t1.organisation_id
    AND t1.person_id = {}
    AND t2.person_id <> {}
    GROUP BY t2.person_id";

pub fn friends(person_id: u64) -> String {
    format!(
        "SELECT person_id2
        FROM person_knows_person
        WHERE person_id1 = {}",
        person_id
    )
}

pub fn common_friends(person_id: u64) -> String {
    format!(
        "SELECT ff.person_id3, COUNT(*) as count
        FROM person_knows_person AS f,
            (SELECT person_id1 AS person_id2, person_id2 AS person_id3 
             FROM person_knows_person
             WHERE person_id1 != {} AND person_id2 != {}) ff
        WHERE f.person_id1 = {} AND f.person_id2 = ff.person_id2
        GROUP BY (f.person_id1, ff.person_id3)
        ORDER BY count DESC",
        person_id, person_id, person_id
    )
}

pub fn work_at(person_id: u64) -> String {
    format!(
        "SELECT t2.person_id, COUNT(*) AS NumCommonOrg
        FROM person_workAt_organisation AS t1, person_workAt_organisation AS t2
        WHERE t2.organisation_id = t1.organisation_id
        AND t1.person_id = {} 
        AND t2.person_id <> {}
        GROUP BY t2.person_id",
        person_id, person_id
    )
}

pub fn study_at(person_id: u64) -> String {
    format!(
        "SELECT t2.person_id, COUNT(*) AS NumCommonOrg
        FROM person_studyAt_organisation AS t1, person_studyAt_organisation AS t2
        WHERE t2.organisation_id = t1.organisation_id
        AND t1.person_id = {} 
        AND t2.person_id <> {}
        GROUP BY t2.person_id",
        person_id, person_id
    )
}
