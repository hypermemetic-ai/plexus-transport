//! `mls-wire-native generate [FILE]` prints the vector file the builder
//! makes, or writes it to FILE.
//! `mls-wire-native report FILE` prints what the native decoder observes of
//! each vector in FILE, and fails if any is not what the file expects.

use std::process::ExitCode;

use mls_wire_native_fixture::{file, report, vectors};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.iter().map(String::as_str).collect::<Vec<_>>()[..] {
        ["generate"] => {
            print!("{}", file::write(&vectors::build()));
            ExitCode::SUCCESS
        }
        ["generate", path] => match std::fs::write(path, file::write(&vectors::build())) {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("{path}: {e}");
                ExitCode::from(2)
            }
        },
        ["report", path] => {
            let vectors = match std::fs::read_to_string(path)
                .map_err(|e| e.to_string())
                .and_then(|text| file::read(&text))
            {
                Ok(vectors) => vectors,
                Err(e) => {
                    eprintln!("{path}: {e}");
                    return ExitCode::from(2);
                }
            };
            let report = report(&vectors);
            print!("{}", report.text);
            for mismatch in &report.mismatches {
                eprintln!("MISMATCH {mismatch}");
            }
            if report.mismatches.is_empty() {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
        _ => {
            eprintln!("usage: mls-wire-native generate [FILE] | report FILE");
            ExitCode::from(2)
        }
    }
}
