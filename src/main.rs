extern crate docopt;
extern crate postgres;
#[macro_use]
extern crate serde_derive;

use docopt::Docopt;
use postgres::{Connection, TlsMode};
use std::{thread, time};

const USAGE: &'static str = "
birdwatcher.

Usage:
  birdwatcher install [-c <connection>] [--tls]
  birdwatcher scan [-i <interval>] [--reset] [-c <connection>] [--tls]
  birdwatcher report [-c <connection>] [--tls]
  birdwatcher (-h | --help)
  birdwatcher --version

Options:
  -h --help                                     Show this screen.
  --version                                     Show version.
  -i <interval>, --interval <interval>          Scan interval in ms [default: 100].
  -c <connection>, --connection <connection>    The connection string [default: postgres://postgres@localhost:5432].
  --tls                                         Enable TLS for database connection.
  --reset                                       Reset the report table.
";

#[derive(Debug, Deserialize)]
struct Args {
    flag_interval: u64,
    flag_reset: bool,
    flag_tls: bool,
    flag_connection: Option<String>,
    cmd_install: bool,
    cmd_scan: bool,
    cmd_report: bool,
}

fn connect(url: String, tls: TlsMode) -> Connection {
    println!("connecting to database {:?}", url);
    return match Connection::connect(url, tls) {
        Ok(connection) => connection,
        Err(error) => {
            eprintln!("oops, there was a problem connecting to the base: {?}", error);
            ::std::process::exit(1);
        }
    };
}

const DROP_TABLE: &'static str = "DROP TABLE IF EXISTS lockTracking";
const CREATE_TABLE: &'static str = "CREATE TABLE IF NOT EXISTS lockTracking (
	mode TEXT,
	pid INTEGER, 
	db TEXT,
	relation TEXT, 
	username TEXT,
	application TEXT, 
	startedAt TIMESTAMP WITH TIME ZONE,
	age INTERVAL, 
	query TEXT
)";

fn install(connection: Connection) {
    println!("installing table lockTracking");
    match connection.execute(DROP_TABLE, &[]) {
        Ok(_) => (),
        Err(error) => eprintln!("couldn't remove table: {:?}", error.to_string()),
    }

    match connection.execute(CREATE_TABLE, &[]) {
        Ok(_) => println!("ready to scan!"),
        Err(error) => {
            eprintln!("oops, couldn't install table: {:?}", error.to_string());
            std::process::exit(1);
        }
    }
}

const INSERT_INTO: &'static str = "INSERT INTO locktracking(mode, pid, db, relation, username, application, startedAt, age, query) 
			SELECT l.mode, l.pid, a.datname, c.relname, a.usename, a.application_name,  a.query_start, age(clock_timestamp(), a.query_start), query
			FROM pg_catalog.pg_locks l JOIN pg_class c ON l.relation = c.oid
			JOIN pg_catalog.pg_stat_activity a ON l.pid = a.pid
			WHERE granted = true AND mode = 'AccessExclusiveLock'";

fn scan(connection: Connection, interval: u64) {
    println!("scanning for locks...");
    let mut i = 0;
    let mut previously_found = 0;
    loop {
        let found = match connection.execute(INSERT_INTO, &[]) {
            Ok(found) => found,
            Err(error) => {
                eprintln!("couldn't scan locks: {:?}", error.to_string());
                0;
            },
        };
        if i == 0 || found != previously_found {
            println!("{} lock(s) found", found);
            previously_found = found;
        }
        i = (i + 1) % 50;
        thread::sleep(time::Duration::from_millis(interval));
    }
}

const REPORT: &'static str =
    "SELECT pid, db, relation, startedAt, query, MAX(age) as duration FROM locktracking
GROUP by pid, db, relation, startedAt, query
ORDER BY startedAt";

struct DetectedLock {
    pid: i32,
    db: String,
    relation: String,
    started_at: String,
    query: String,
    age: String,
}

fn report(connection: Connection) {
    let mut i = 0;
    let rows = connection.query(REPORT, &[]).unwrap();
    if rows.len() == 0 {
        println!("no lock have been detected 🎉");
        return;
    }
    for row in &rows {
        let lock = DetectedLock {
            pid: row.get(0),
            db: row.get(1),
            relation: row.get(2),
            started_at: row.get(3),
            query: row.get(4),
            age: row.get(5),
        };
        println!("🔒{}\t{}\t{}\t{}\t{}\t{}\t{}", i, lock.pid, lock.db, lock.relation, lock.started_at, lock.query, lock.age);
        i = i + 1;
    }
}

fn main() {
    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.deserialize())
        .unwrap_or_else(|e| e.exit());
    println!("{:?}", args);
    let connection = connect(args.flag_connection.unwrap(), TlsMode::None);
    if args.cmd_install {
        install(connection);
    } else if args.cmd_scan {
        scan(connection, args.flag_interval);
    } else if args.cmd_report {
        report(connection);
    } else {
        panic!("No command specified");
    }
}
