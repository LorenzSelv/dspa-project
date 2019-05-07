extern crate postgres;

use postgres::{Connection, TlsMode};

pub fn connect() {
    let conn = Connection::connect("postgres://postgres:postgres@localhost:5432",
                                   TlsMode::None).unwrap();
    conn.execute("CREATE TABLE IF NOT EXISTS person_knows_person (
                      person1 integer NOT NULL,
                      person2 integer NOT NULL,
                      PRIMARY KEY (person1, person2)
                  )", &[]).unwrap();

    // DELETE al rows from the table and repoulate
    conn.execute("DELETE FROM person_knows_person", &[]).unwrap();

    conn.execute("COPY person_knows_person
                  FROM '/home/sara/ETH/DSPA/dspa-project/dataset/1k-users-sorted/tables/person_knows_person.csv'
                  DELIMITERS '|' CSV HEADER;", &[]).unwrap();

    // example query.
    // find friends of user 900
    for row in &conn.query("SELECT person2 FROM person_knows_person WHERE person1 = 900",
                           &[]).unwrap() {
        let friend : i32 = row.get(0);
        println!("Found friend {}", friend);
    }
}
