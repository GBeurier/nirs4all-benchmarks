use std::{
    collections::BTreeMap,
    fs,
    io::{self, Read},
    path::Path,
    time::Instant,
};

use nirs4all::{
    dag_ml::RunId, inspect_methods_archive_v2_predictors, load_archive_v2,
    predict_methods_archive_v2_matrix, MethodsArchiveMatrixPredictRequest,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

const PROTOCOL: &str = "nirs4all.performance-compare.adapter.v1";
const CORE_COMMIT: &str = "550cb8c80708e88ac7ebbc880acb4b82d8531632";
const METHODS_COMMIT: &str = "48ad1e5a50844f68c2b99e93b02ad6a3b491c07b";
const METHODS_LIBRARY: &str = env!("PERF001_METHODS_LIBRARY");

fn strings(value: &Value, key: &str) -> Result<Vec<String>, String> {
    serde_json::from_value(value[key].clone()).map_err(|error| format!("invalid {key}: {error}"))
}

fn matrix(value: &Value) -> Result<Vec<Vec<f64>>, String> {
    serde_json::from_value(value.clone()).map_err(|error| format!("invalid matrix: {error}"))
}

fn predict(
    archive: &nirs4all::LoadedArchiveV2,
    request: &Value,
    library_sha256: &str,
    suffix: &str,
) -> Result<Value, String> {
    let matrix_request = &request["matrix"];
    let outcome = predict_methods_archive_v2_matrix(
        archive,
        MethodsArchiveMatrixPredictRequest {
            sample_ids: strings(matrix_request, "sample_ids")?,
            x: matrix(&matrix_request["x"])?,
            expected_target_names: strings(matrix_request, "target_names")?,
            methods_library_path: Path::new(METHODS_LIBRARY).to_path_buf(),
            methods_library_sha256: library_sha256.to_owned(),
            request_id: format!("request:perf001.rust.{suffix}"),
            outcome_id: format!("outcome:perf001.rust.{suffix}"),
            run_id: RunId::new(format!("run:perf001.rust.{suffix}"))
                .map_err(|error| error.to_string())?,
            warnings: Vec::new(),
            diagnostics: BTreeMap::from([(
                "contract".to_owned(),
                Value::String("nirs4all.performance-compare.adapter.v1".to_owned()),
            )]),
        },
    )
    .map_err(|error| error.to_string())?;
    serde_json::to_value(outcome).map_err(|error| error.to_string())
}

fn run() -> Result<Value, String> {
    let started = Instant::now();
    let mut input = String::new();
    io::stdin()
        .read_to_string(&mut input)
        .map_err(|error| error.to_string())?;
    let request: Value = serde_json::from_str(&input).map_err(|error| error.to_string())?;
    if request["protocol"] != PROTOCOL || request["surface"] != "rust_direct" {
        return Err("unexpected PERF-001 adapter request".to_owned());
    }
    if request["candidate"]["commit_sha"] != CORE_COMMIT {
        return Err("Core candidate mismatch".to_owned());
    }
    let library = fs::read(METHODS_LIBRARY).map_err(|error| error.to_string())?;
    let library_sha256 = format!("{:x}", Sha256::digest(&library));
    let archive_path = request["archive_v2"]["path"]
        .as_str()
        .ok_or("archive path is missing")?;
    let archive = load_archive_v2(Path::new(archive_path)).map_err(|error| error.to_string())?;
    let descriptors = inspect_methods_archive_v2_predictors(
        &archive,
        Path::new(METHODS_LIBRARY),
        &library_sha256,
    )
    .map_err(|error| error.to_string())?;
    if descriptors.len() != 1
        || descriptors[0].descriptor_fingerprint != request["predictor_fingerprint"]
    {
        return Err("Core descriptor fingerprint mismatch".to_owned());
    }

    let mut outcome = predict(&archive, &request, &library_sha256, "startup")?;
    let startup_ms = started.elapsed().as_secs_f64() * 1000.0;
    let repeats = request["repeats"].as_u64().ok_or("repeats is missing")?;
    let mut steady_state_ms = Vec::new();
    for repeat in 0..repeats {
        let before = Instant::now();
        outcome = predict(&archive, &request, &library_sha256, &repeat.to_string())?;
        steady_state_ms.push(before.elapsed().as_secs_f64() * 1000.0);
    }
    let block = &outcome["outputs"][0]["predictions"][0];
    Ok(json!({
        "protocol": PROTOCOL,
        "surface": "rust_direct",
        "commit_sha": CORE_COMMIT,
        "archive_sha256": request["archive_v2"]["sha256"],
        "matrix_sha256": request["matrix_sha256"],
        "sample_ids": request["matrix"]["sample_ids"],
        "target_names": request["matrix"]["target_names"],
        "predictor_descriptor": request["predictor_descriptor"],
        "predictor_fingerprint": request["predictor_fingerprint"],
        "fallback_used": false,
        "predictions": block["values"],
        "startup_ms": startup_ms,
        "steady_state_ms": steady_state_ms,
        "evidence": {
            "kind": "local_synthetic_current_head",
            "entrypoint": "nirs4all::predict_methods_archive_v2_matrix",
            "core_commit": CORE_COMMIT,
            "methods_commit": METHODS_COMMIT,
            "methods_library_sha256": library_sha256,
            "python": false,
        },
    }))
}

fn main() {
    match run() {
        Ok(output) => println!("{output}"),
        Err(error) => {
            eprintln!("PERF-001 Rust adapter: {error}");
            std::process::exit(2);
        }
    }
}
