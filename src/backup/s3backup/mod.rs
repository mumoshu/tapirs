use aws_sdk_s3::Client;
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::error::ProvideErrorMetadata;

use super::storage::BackupStorage;

/// Check if a PutObject error is an HTTP 412 PreconditionFailed.
/// S3 returns this for If-None-Match / If-Match precondition failures.
/// The SDK maps it to the error's metadata code.
fn is_precondition_failed(err: &PutObjectError) -> bool {
    let code = err.code().unwrap_or("");
    code == "PreconditionFailed" || code == "ConditionalRequestConflict"
}

/// S3-compatible backup storage backend.
///
/// Uses the AWS SDK async client with standard credential chain.
/// Supports custom regions and S3-compatible endpoints (e.g. MinIO).
pub struct S3BackupStorage {
    client: Client,
    bucket: String,
    prefix: String,
}

impl S3BackupStorage {
    /// Create a new S3 storage backend.
    ///
    /// When `endpoint_url` is set, enables path-style access (required by MinIO
    /// and most S3-compatible services).
    pub async fn new(
        bucket: &str,
        prefix: &str,
        region: Option<&str>,
        endpoint_url: Option<&str>,
    ) -> Self {
        let mut config_loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
        // S3 requires a region. When using a custom endpoint (MinIO etc.)
        // without an explicit region, default to us-east-1.
        let effective_region = region
            .map(|r| r.to_string())
            .or_else(|| endpoint_url.map(|_| "us-east-1".to_string()));
        if let Some(r) = effective_region {
            config_loader = config_loader.region(aws_config::Region::new(r));
        }
        if let Some(ep) = endpoint_url {
            config_loader = config_loader.endpoint_url(ep);
        }
        let sdk_config = config_loader.load().await;

        let mut s3_config = aws_sdk_s3::config::Builder::from(&sdk_config);
        if endpoint_url.is_some() {
            s3_config = s3_config.force_path_style(true);
        }
        let client = Client::from_conf(s3_config.build());

        Self {
            client,
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
        }
    }

    /// Parse an `s3://bucket/prefix/` URI into `(bucket, prefix)`.
    ///
    /// The prefix always ends with `/` (or is empty for bucket root).
    pub fn parse_s3_uri(uri: &str) -> Result<(String, String), String> {
        let rest = uri
            .strip_prefix("s3://")
            .ok_or_else(|| format!("not an S3 URI: {uri}"))?;
        if rest.is_empty() {
            return Err("S3 URI missing bucket name".to_string());
        }
        let (bucket, prefix) = match rest.find('/') {
            Some(i) => {
                let b = &rest[..i];
                let mut p = rest[i + 1..].to_string();
                if !p.is_empty() && !p.ends_with('/') {
                    p.push('/');
                }
                (b.to_string(), p)
            }
            None => (rest.to_string(), String::new()),
        };
        if bucket.is_empty() {
            return Err("S3 URI has empty bucket name".to_string());
        }
        Ok((bucket, prefix))
    }

    fn key(&self, name: &str) -> String {
        format!("{}{name}", self.prefix)
    }
}

impl BackupStorage for S3BackupStorage {
    async fn init(&self) -> Result<(), String> {
        // S3 buckets are pre-created; nothing to initialize.
        Ok(())
    }

    async fn exists(&self, name: &str) -> Result<bool, String> {
        match self.client.head_object()
            .bucket(&self.bucket)
            .key(self.key(name))
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(err) => {
                let svc_err = err.into_service_error();
                if svc_err.is_not_found() {
                    Ok(false)
                } else {
                    Err(format!("S3 HeadObject {name}: {svc_err}"))
                }
            }
        }
    }

    async fn size(&self, name: &str) -> Result<Option<u64>, String> {
        match self.client.head_object()
            .bucket(&self.bucket)
            .key(self.key(name))
            .send()
            .await
        {
            Ok(resp) => Ok(Some(resp.content_length().unwrap_or(0) as u64)),
            Err(err) => {
                let svc_err = err.into_service_error();
                if svc_err.is_not_found() {
                    Ok(None)
                } else {
                    Err(format!("S3 HeadObject {name}: {svc_err}"))
                }
            }
        }
    }

    async fn read(&self, name: &str) -> Result<Vec<u8>, String> {
        let resp = self.client.get_object()
            .bucket(&self.bucket)
            .key(self.key(name))
            .send()
            .await
            .map_err(|e| format!("S3 GetObject {name}: {e}"))?;
        let bytes = resp.body.collect()
            .await
            .map_err(|e| format!("S3 read body {name}: {e}"))?;
        Ok(bytes.into_bytes().to_vec())
    }

    async fn write(&self, name: &str, data: &[u8]) -> Result<(), String> {
        self.client.put_object()
            .bucket(&self.bucket)
            .key(self.key(name))
            .body(ByteStream::from(data.to_vec()))
            .send()
            .await
            .map_err(|e| format!("S3 PutObject {name}: {e}"))?;
        Ok(())
    }

    async fn create_if_absent(&self, name: &str, data: &[u8]) -> Result<bool, String> {
        match self.client.put_object()
            .bucket(&self.bucket)
            .key(self.key(name))
            .if_none_match("*")
            .body(ByteStream::from(data.to_vec()))
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(err) => {
                let svc_err = err.into_service_error();
                // HTTP 412 PreconditionFailed means the object already exists.
                // The SDK maps this to Unhandled — check the error code.
                if is_precondition_failed(&svc_err) {
                    Ok(false)
                } else {
                    Err(format!("S3 PutObject (create_if_absent) {name}: {svc_err}"))
                }
            }
        }
    }

    async fn write_if_larger(&self, name: &str, data: &[u8]) -> Result<bool, String> {
        let local_size = data.len() as u64;
        loop {
            // HEAD to get current size + ETag.
            let (remote_size, etag) = match self.client.head_object()
                .bucket(&self.bucket)
                .key(self.key(name))
                .send()
                .await
            {
                Ok(resp) => {
                    let size = resp.content_length().unwrap_or(0) as u64;
                    let etag = resp.e_tag().unwrap_or("").to_string();
                    (size, etag)
                }
                Err(err) => {
                    let svc_err = err.into_service_error();
                    if svc_err.is_not_found() {
                        // Object doesn't exist — unconditional write.
                        self.write(name, data).await?;
                        return Ok(true);
                    }
                    return Err(format!("S3 HeadObject (write_if_larger) {name}: {svc_err}"));
                }
            };

            if local_size <= remote_size {
                return Ok(false);
            }

            // CAS: PUT with If-Match to ensure no concurrent write.
            match self.client.put_object()
                .bucket(&self.bucket)
                .key(self.key(name))
                .if_match(&etag)
                .body(ByteStream::from(data.to_vec()))
                .send()
                .await
            {
                Ok(_) => return Ok(true),
                Err(err) => {
                    let svc_err = err.into_service_error();
                    if is_precondition_failed(&svc_err) {
                        // Another writer changed the object — retry CAS loop.
                        continue;
                    }
                    return Err(format!("S3 PutObject (write_if_larger) {name}: {svc_err}"));
                }
            }
        }
    }

    async fn list_files(&self, prefix: &str) -> Result<Vec<String>, String> {
        let full_prefix = format!("{}{prefix}", self.prefix);
        let mut files = Vec::new();
        let mut continuation_token: Option<String> = None;
        loop {
            let mut req = self.client.list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&full_prefix);
            if let Some(ref token) = continuation_token {
                req = req.continuation_token(token);
            }
            let resp = req.send().await
                .map_err(|e| format!("S3 ListObjectsV2 (list_files): {e}"))?;
            if let Some(contents) = resp.contents {
                for obj in contents {
                    if let Some(key) = obj.key {
                        let relative = key.strip_prefix(&self.prefix).unwrap_or(&key);
                        files.push(relative.to_string());
                    }
                }
            }
            if resp.is_truncated.unwrap_or(false) {
                continuation_token = resp.next_continuation_token;
            } else {
                break;
            }
        }
        files.sort();
        Ok(files)
    }

    async fn list_subdirs(&self) -> Result<Vec<String>, String> {
        let resp = self.client.list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&self.prefix)
            .delimiter("/")
            .send()
            .await
            .map_err(|e| format!("S3 ListObjectsV2: {e}"))?;

        let mut dirs = Vec::new();
        if let Some(prefixes) = resp.common_prefixes {
            for cp in prefixes {
                if let Some(p) = cp.prefix {
                    // Strip our prefix and trailing '/'.
                    let relative = p.strip_prefix(&self.prefix).unwrap_or(&p);
                    let name = relative.trim_end_matches('/');
                    if !name.is_empty() {
                        dirs.push(name.to_string());
                    }
                }
            }
        }
        Ok(dirs)
    }

    fn sub(&self, name: &str) -> Self {
        let prefix = if name.is_empty() {
            self.prefix.clone()
        } else {
            format!("{}{name}/", self.prefix)
        };
        Self {
            client: self.client.clone(),
            bucket: self.bucket.clone(),
            prefix,
        }
    }

    fn display_path(&self) -> String {
        format!("s3://{}/{}", self.bucket, self.prefix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_s3_uri_valid() {
        let (bucket, prefix) = S3BackupStorage::parse_s3_uri("s3://my-bucket/backups/").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "backups/");

        let (bucket, prefix) = S3BackupStorage::parse_s3_uri("s3://my-bucket/a/b/c").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "a/b/c/");

        let (bucket, prefix) = S3BackupStorage::parse_s3_uri("s3://my-bucket").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "");

        let (bucket, prefix) = S3BackupStorage::parse_s3_uri("s3://my-bucket/").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "");
    }

    #[test]
    fn parse_s3_uri_invalid() {
        assert!(S3BackupStorage::parse_s3_uri("http://foo").is_err());
        assert!(S3BackupStorage::parse_s3_uri("s3://").is_err());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn s3_storage_roundtrip() {
        let storage = crate::storage::remote::test_helpers::minio::test_s3_storage(
            "s3-storage-roundtrip",
        )
        .await;

        // init is a no-op for S3.
        storage.init().await.unwrap();

        // Write and read back.
        storage.write("hello.txt", b"world").await.unwrap();
        assert!(storage.exists("hello.txt").await.unwrap());
        let data = storage.read("hello.txt").await.unwrap();
        assert_eq!(data, b"world");

        // Non-existent file.
        assert!(!storage.exists("no-such-file.txt").await.unwrap());

        // Sub-directory and list_subdirs.
        let sub = storage.sub("subdir");
        sub.write("nested.txt", b"data").await.unwrap();
        let dirs = storage.list_subdirs().await.unwrap();
        assert!(dirs.contains(&"subdir".to_string()), "dirs: {dirs:?}");

        // read_string default impl.
        let text = storage.read_string("hello.txt").await.unwrap();
        assert_eq!(text, "world");
    }
}
