use std::process::Command;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use aws_sdk_s3::error::ProvideErrorMetadata;

use crate::backup::s3backup::S3BackupStorage;

const ACCESS_KEY: &str = "minioadmin";
const SECRET_KEY: &str = "minioadmin";
const CONTAINER_LABEL: &str = "tapi-rs-minio-test";

struct SharedMinio {
    endpoint: String,
}

static SHARED: OnceLock<SharedMinio> = OnceLock::new();

/// Get the shared MinIO endpoint. Starts the container on first call.
pub fn minio_endpoint() -> &'static str {
    &SHARED.get_or_init(start_minio).endpoint
}

fn start_minio() -> SharedMinio {
    // Set AWS credentials for the AWS SDK to authenticate with MinIO.
    // SAFETY: called once during OnceLock init, before any multi-threaded
    // AWS SDK usage. No concurrent readers of these env vars at this point.
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", ACCESS_KEY);
        std::env::set_var("AWS_SECRET_ACCESS_KEY", SECRET_KEY);
    }

    // Remove any leftover container from a crashed previous run.
    let _ = Command::new("docker")
        .args(["rm", "-f", CONTAINER_LABEL])
        .output();

    let output = Command::new("docker")
        .args([
            "run", "-d",
            "--name", CONTAINER_LABEL,
            "-P",
            "-e", &format!("MINIO_ROOT_USER={ACCESS_KEY}"),
            "-e", &format!("MINIO_ROOT_PASSWORD={SECRET_KEY}"),
            "minio/minio", "server", "/data",
        ])
        .output()
        .expect("failed to start MinIO container — is Docker running?");
    assert!(
        output.status.success(),
        "docker run failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let container_id = String::from_utf8(output.stdout)
        .expect("non-utf8 container id")
        .trim()
        .to_string();

    // Discover the dynamically-assigned host port.
    let port_output = Command::new("docker")
        .args(["port", &container_id, "9000"])
        .output()
        .expect("docker port failed");
    let port_str = String::from_utf8(port_output.stdout).expect("non-utf8 port");
    // Format: "0.0.0.0:XXXXX\n" or "[::]:XXXXX\n"
    let host_port = port_str
        .trim()
        .rsplit(':')
        .next()
        .expect("no port in docker port output");
    let endpoint = format!("http://127.0.0.1:{host_port}");

    wait_ready(&endpoint);

    // Cleanup thread: waits for all sender handles to drop (process exit),
    // then removes the container. The fixed container name also means the
    // next test run's cleanup-by-name handles leftover containers from SIGKILL.
    std::thread::spawn(move || {
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        std::mem::forget(tx);
        let _ = rx.recv();
        let _ = Command::new("docker")
            .args(["rm", "-f", &container_id])
            .output();
    });

    SharedMinio { endpoint }
}

fn wait_ready(endpoint: &str) {
    let url = format!("{endpoint}/minio/health/ready");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match ureq::get(&url).call() {
            Ok(resp) if resp.status() == 200 => return,
            _ => {}
        }
        assert!(
            Instant::now() < deadline,
            "MinIO not ready within 10s at {endpoint}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Create a fresh, empty bucket for a test. Async — call from within a
/// tokio runtime. If the bucket exists from a crashed run, empties and
/// recreates it.
pub async fn create_test_bucket(test_name: &str) -> String {
    let bucket = sanitize_bucket_name(test_name);
    let endpoint = minio_endpoint();
    let client = build_raw_client(endpoint).await;
    match client.create_bucket().bucket(&bucket).send().await {
        Ok(_) => {}
        Err(e) => {
            // MinIO returns BucketAlreadyOwnedByYou; check the service error code.
            let is_already_exists = e
                .as_service_error()
                .map(|se| {
                    let code = se.code().unwrap_or("");
                    code == "BucketAlreadyOwnedByYou" || code == "BucketAlreadyExists"
                })
                .unwrap_or(false);
            if !is_already_exists {
                panic!("create_bucket({bucket}) failed: {e:?}");
            }
            empty_bucket(&client, &bucket).await;
        }
    }
    bucket
}

fn sanitize_bucket_name(test_name: &str) -> String {
    let bucket: String = test_name
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c.to_ascii_lowercase() } else { '-' })
        .collect();
    let bucket = bucket.trim_matches('-').to_string();
    assert!(!bucket.is_empty(), "test_name produced empty bucket name");
    bucket
}

async fn build_raw_client(endpoint: &str) -> aws_sdk_s3::Client {
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new("us-east-1"))
        .endpoint_url(endpoint)
        .load()
        .await;
    let s3_config = aws_sdk_s3::config::Builder::from(&config)
        .force_path_style(true)
        .build();
    aws_sdk_s3::Client::from_conf(s3_config)
}

async fn empty_bucket(client: &aws_sdk_s3::Client, bucket: &str) {
    let mut continuation = None;
    loop {
        let mut req = client.list_objects_v2().bucket(bucket);
        if let Some(token) = continuation.take() {
            req = req.continuation_token(token);
        }
        let resp = match req.send().await {
            Ok(r) => r,
            Err(_) => return,
        };
        if let Some(contents) = resp.contents {
            for obj in &contents {
                if let Some(key) = &obj.key {
                    let _ = client.delete_object().bucket(bucket).key(key).send().await;
                }
            }
        }
        if resp.is_truncated == Some(true) {
            continuation = resp.next_continuation_token;
        } else {
            break;
        }
    }
}

/// Build an S3BackupStorage pointing at the shared MinIO with a fresh
/// test-specific bucket. Async — call from within a tokio runtime.
pub async fn test_s3_storage(test_name: &str) -> S3BackupStorage {
    let bucket = create_test_bucket(test_name).await;
    let endpoint = minio_endpoint();
    S3BackupStorage::new(&bucket, "", None, Some(endpoint)).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backup::storage::BackupStorage;

    #[test]
    fn shared_minio_starts_once() {
        let ep1 = minio_endpoint();
        let ep2 = minio_endpoint();
        assert_eq!(ep1, ep2, "should return same endpoint");
        assert!(ep1.starts_with("http://127.0.0.1:"), "unexpected endpoint: {ep1}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn create_bucket_and_use() {
        let storage = test_s3_storage("create-bucket-and-use").await;
        storage.write("test.txt", b"hello").await.unwrap();
        let data = storage.read("test.txt").await.unwrap();
        assert_eq!(data, b"hello");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stale_bucket_cleaned() {
        // Write data to a bucket.
        let storage = test_s3_storage("stale-bucket-cleaned").await;
        storage.write("stale.txt", b"old").await.unwrap();

        // Recreate the same bucket — should clean the stale data.
        let bucket = create_test_bucket("stale-bucket-cleaned").await;
        let endpoint = minio_endpoint();
        let storage = S3BackupStorage::new(&bucket, "", None, Some(endpoint)).await;
        assert!(
            !storage.exists("stale.txt").await.unwrap(),
            "stale object should be gone"
        );
    }
}
