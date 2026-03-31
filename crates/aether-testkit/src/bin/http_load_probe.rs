use std::time::Duration;

use aether_testkit::{run_http_load_probe, HttpLoadProbeConfig};
use reqwest::Method;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = parse_args(std::env::args().skip(1).collect())?;
    let result = run_http_load_probe(&config)
        .await
        .map_err(std::io::Error::other)?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

fn parse_args(args: Vec<String>) -> Result<HttpLoadProbeConfig, Box<dyn std::error::Error>> {
    let mut url: Option<String> = None;
    let mut total_requests: Option<usize> = None;
    let mut concurrency: Option<usize> = None;
    let mut timeout_ms: Option<u64> = None;
    let mut method = Method::GET;

    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--url" => url = Some(next_value(&mut iter, "--url")?),
            "--requests" => total_requests = Some(next_value(&mut iter, "--requests")?.parse()?),
            "--concurrency" => concurrency = Some(next_value(&mut iter, "--concurrency")?.parse()?),
            "--timeout-ms" => timeout_ms = Some(next_value(&mut iter, "--timeout-ms")?.parse()?),
            "--method" => {
                method = Method::from_bytes(next_value(&mut iter, "--method")?.as_bytes())?
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            other => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("unknown argument: {other}"),
                )
                .into());
            }
        }
    }

    let mut config = HttpLoadProbeConfig {
        url: url.ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "missing required --url")
        })?,
        total_requests: total_requests.ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "missing required --requests",
            )
        })?,
        concurrency: concurrency.ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "missing required --concurrency",
            )
        })?,
        method,
        ..HttpLoadProbeConfig::default()
    };
    if let Some(timeout_ms) = timeout_ms {
        config.timeout = Duration::from_millis(timeout_ms);
    }
    config
        .validate()
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidInput, err))?;
    Ok(config)
}

fn next_value(
    iter: &mut impl Iterator<Item = String>,
    flag: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    iter.next().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("missing value for {flag}"),
        )
        .into()
    })
}

fn print_usage() {
    eprintln!(
        "usage: cargo run -p aether-testkit --bin http_load_probe -- --url <URL> --requests <N> --concurrency <N> [--method GET] [--timeout-ms 30000]"
    );
}
