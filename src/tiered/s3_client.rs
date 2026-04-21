// When cloud feature is disabled, S3Client is a zero-size type that can never
// be constructed. The type exists so Option<Arc<S3Client>> compiles everywhere.
#[cfg(not(feature = "cloud"))]
pub(crate) struct S3Client {
    _private: (),  // prevent construction
}

#[cfg(feature = "cloud")]
use super::*;

// ===== S3Client (sync wrapper around async SDK) =====

#[cfg(feature = "cloud")]
/// Synchronous S3 client wrapping the async AWS SDK.
pub(crate) struct S3Client {
    pub(crate) client: aws_sdk_s3::Client,
    pub(crate) bucket: String,
    pub(crate) prefix: String,
    pub(crate) runtime: TokioHandle,
    /// S3 GET count (for benchmarking / diagnostics)
    pub(crate) fetch_count: AtomicU64,
    /// S3 bytes fetched (for benchmarking / diagnostics)
    pub(crate) fetch_bytes: AtomicU64,
    /// S3 PUT count (for benchmarking / diagnostics)
    pub(crate) put_count: AtomicU64,
    /// S3 bytes uploaded (for benchmarking / diagnostics)
    pub(crate) put_bytes: AtomicU64,
}

#[cfg(feature = "cloud")]
impl S3Client {
    /// Create a new S3 client.
    pub(crate) async fn new_async(config: &TurboliteConfig) -> io::Result<Self> {
        turbolite_debug!("[s3] new_async: loading aws_config...");
        let mut aws_config = aws_config::from_env();

        if let Some(region) = &config.region {
            turbolite_debug!("[s3] setting region: {}", region);
            aws_config =
                aws_config.region(aws_sdk_s3::config::Region::new(region.clone()));
        }

        let aws_config = aws_config.load().await;
        turbolite_debug!("[s3] aws_config loaded");

        let mut s3_config = aws_sdk_s3::config::Builder::from(&aws_config);
        if let Some(endpoint) = &config.endpoint_url {
            turbolite_debug!("[s3] setting endpoint: {}", endpoint);
            s3_config = s3_config.endpoint_url(endpoint).force_path_style(true);
        } else {
            turbolite_debug!("[s3] using default AWS S3 endpoint");
        }

        let client = aws_sdk_s3::Client::from_conf(s3_config.build());
        turbolite_debug!("[s3] S3 client created");

        let runtime = config
            .runtime_handle
            .clone()
            .or_else(|| TokioHandle::try_current().ok())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::Other,
                    "No tokio runtime available. Pass runtime_handle in TurboliteConfig \
                     or call from within a tokio context.",
                )
            })?;

        Ok(Self {
            client,
            bucket: config.bucket.clone(),
            prefix: config.prefix.clone(),
            runtime,
            fetch_count: AtomicU64::new(0),
            fetch_bytes: AtomicU64::new(0),
            put_count: AtomicU64::new(0),
            put_bytes: AtomicU64::new(0),
        })
    }

    /// Blocking constructor.
    pub(crate) fn new_blocking(config: &TurboliteConfig, runtime: &TokioHandle) -> io::Result<Self> {
        Self::block_on(runtime, Self::new_async(config))
    }

    /// Run an async future on the tokio runtime, handling both
    /// "inside tokio" and "outside tokio" cases.
    pub(crate) fn block_on<F: std::future::Future<Output = T>, T>(
        handle: &TokioHandle,
        fut: F,
    ) -> T {
        match TokioHandle::try_current() {
            Ok(_) => tokio::task::block_in_place(|| handle.block_on(fut)),
            Err(_) => handle.block_on(fut),
        }
    }

    pub(crate) fn s3_key(&self, suffix: &str) -> String {
        if self.prefix.is_empty() {
            suffix.to_string()
        } else {
            format!("{}/{}", self.prefix, suffix)
        }
    }

    pub(crate) fn manifest_key_msgpack(&self) -> String {
        self.s3_key("manifest.msgpack")
    }

    pub(crate) fn manifest_key_json(&self) -> String {
        self.s3_key("manifest.json")
    }

    /// Generate versioned S3 key for a data page group.
    pub(crate) fn page_group_key(&self, group_id: u64, version: u64) -> String {
        self.s3_key(&format!("p/d/{}_v{}", group_id, version))
    }

    /// Generate versioned S3 key for an interior bundle.
    pub(crate) fn interior_chunk_key(&self, chunk_id: u32, version: u64) -> String {
        self.s3_key(&format!("p/it/{}_v{}", chunk_id, version))
    }

    /// Generate versioned S3 key for an index leaf bundle.
    pub(crate) fn index_chunk_key(&self, chunk_id: u32, version: u64) -> String {
        self.s3_key(&format!("p/ix/{}_v{}", chunk_id, version))
    }

    /// Phase Drift: override frame key.
    pub(crate) fn override_frame_key(&self, group_id: u64, frame_idx: usize, version: u64) -> String {
        self.s3_key(&format!("p/d/{}_f{}_v{}", group_id, frame_idx, version))
    }

    // --- Generic GET/PUT ---

    pub(crate) async fn get_object_async(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        let mut retries = 0u32;
        loop {
            match self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
                .await
            {
                Ok(resp) => {
                    let bytes = resp
                        .body
                        .collect()
                        .await
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
                        .into_bytes();
                    self.fetch_count.fetch_add(1, Ordering::Relaxed);
                    self.fetch_bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                    return Ok(Some(bytes.to_vec()));
                }
                Err(e) => {
                    if is_not_found(&e) {
                        return Ok(None);
                    }
                    retries += 1;
                    if retries >= 3 {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!("S3 GET {} failed after 3 retries: {:?}", key, e),
                        ));
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(
                        100 * (1 << retries),
                    ))
                    .await;
                }
            }
        }
    }

    /// Byte-range GET: fetch [start..start+len) from an S3 object.
    pub(crate) async fn range_get_async(&self, key: &str, start: u64, len: u32) -> io::Result<Option<Vec<u8>>> {
        let range = format!("bytes={}-{}", start, start + len as u64 - 1);
        let mut retries = 0u32;
        loop {
            match self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(key)
                .range(&range)
                .send()
                .await
            {
                Ok(resp) => {
                    let bytes = resp
                        .body
                        .collect()
                        .await
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
                        .into_bytes();
                    self.fetch_count.fetch_add(1, Ordering::Relaxed);
                    self.fetch_bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                    return Ok(Some(bytes.to_vec()));
                }
                Err(e) => {
                    if is_not_found(&e) {
                        return Ok(None);
                    }
                    retries += 1;
                    if retries >= 3 {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!("S3 range GET {} ({}) failed after 3 retries: {:?}", key, range, e),
                        ));
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(
                        100 * (1 << retries),
                    ))
                    .await;
                }
            }
        }
    }

    /// Blocking byte-range GET.
    pub(crate) fn range_get(&self, key: &str, start: u64, len: u32) -> io::Result<Option<Vec<u8>>> {
        S3Client::block_on(&self.runtime, self.range_get_async(key, start, len))
    }

    pub(crate) async fn put_object_async(&self, key: &str, data: Vec<u8>, content_type: Option<&str>) -> io::Result<()> {
        let mut retries = 0u32;
        loop {
            let body = aws_sdk_s3::primitives::ByteStream::from(data.clone());
            let mut req = self
                .client
                .put_object()
                .bucket(&self.bucket)
                .key(key)
                .body(body);
            if let Some(ct) = content_type {
                req = req.content_type(ct);
            }
            let data_len = data.len() as u64;
            match req.send().await {
                Ok(_) => {
                    self.put_count.fetch_add(1, Ordering::Relaxed);
                    self.put_bytes.fetch_add(data_len, Ordering::Relaxed);
                    return Ok(());
                }
                Err(e) => {
                    retries += 1;
                    if retries >= 3 {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!("S3 PUT {} failed after 3 retries: {}", key, e),
                        ));
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(
                        100 * (1 << retries),
                    ))
                    .await;
                }
            }
        }
    }

    // --- Page group operations ---

    /// Fetch a page group by its S3 key.
    pub(crate) fn get_page_group(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        S3Client::block_on(&self.runtime, self.get_object_async(key))
    }

    /// Fetch multiple page groups in parallel by S3 key.
    /// Returns key → bytes for found objects.
    #[allow(dead_code)]
    pub(crate) fn get_page_groups_by_key(&self, keys: &[String]) -> io::Result<HashMap<String, Vec<u8>>> {
        S3Client::block_on(&self.runtime, self.get_page_groups_by_key_async(keys))
    }

    #[allow(dead_code)]
    pub(crate) async fn get_page_groups_by_key_async(&self, keys: &[String]) -> io::Result<HashMap<String, Vec<u8>>> {
        let mut handles = Vec::with_capacity(keys.len());
        for key in keys {
            let client = self.client.clone();
            let bucket = self.bucket.clone();
            let key = key.clone();
            handles.push(tokio::spawn(async move {
                let mut retries = 0u32;
                loop {
                    match client
                        .get_object()
                        .bucket(&bucket)
                        .key(&key)
                        .send()
                        .await
                    {
                        Ok(resp) => {
                            let bytes = resp
                                .body
                                .collect()
                                .await
                                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
                                .into_bytes();
                            return Ok::<_, io::Error>((key, Some(bytes.to_vec())));
                        }
                        Err(e) => {
                            if is_not_found(&e) {
                                return Ok((key, None));
                            }
                            retries += 1;
                            if retries >= 3 {
                                return Err(io::Error::new(
                                    io::ErrorKind::Other,
                                    format!("S3 GET {} failed: {}", key, e),
                                ));
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(
                                100 * (1 << retries),
                            ))
                            .await;
                        }
                    }
                }
            }));
        }

        let mut result = HashMap::new();
        for handle in handles {
            let (key, data) = handle
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))??;
            if let Some(bytes) = data {
                self.fetch_count.fetch_add(1, Ordering::Relaxed);
                self.fetch_bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                result.insert(key, bytes);
            }
        }
        Ok(result)
    }

    /// Upload multiple page groups in parallel. Each entry is (s3_key, raw_bytes).
    pub(crate) fn put_page_groups(&self, groups: &[(String, Vec<u8>)]) -> io::Result<()> {
        S3Client::block_on(&self.runtime, self.put_page_groups_async(groups))
    }

    pub(crate) async fn put_page_groups_async(&self, groups: &[(String, Vec<u8>)]) -> io::Result<()> {
        let total_bytes: u64 = groups.iter().map(|(_, d)| d.len() as u64).sum();
        let total_puts = groups.len() as u64;
        let mut handles = Vec::with_capacity(groups.len());
        for (key, data) in groups {
            let client = self.client.clone();
            let bucket = self.bucket.clone();
            let key = key.clone();
            let data = data.clone();
            handles.push(tokio::spawn(async move {
                let mut retries = 0u32;
                loop {
                    let body =
                        aws_sdk_s3::primitives::ByteStream::from(data.clone());
                    match client
                        .put_object()
                        .bucket(&bucket)
                        .key(&key)
                        .body(body)
                        .send()
                        .await
                    {
                        Ok(_) => return Ok::<_, io::Error>(()),
                        Err(e) => {
                            retries += 1;
                            if retries >= 3 {
                                return Err(io::Error::new(
                                    io::ErrorKind::Other,
                                    format!("S3 PUT {} failed: {}", key, e),
                                ));
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(
                                100 * (1 << retries),
                            ))
                            .await;
                        }
                    }
                }
            }));
        }

        for handle in handles {
            handle
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))??;
        }
        // All PUTs succeeded; update counters
        self.put_count.fetch_add(total_puts, Ordering::Relaxed);
        self.put_bytes.fetch_add(total_bytes, Ordering::Relaxed);
        Ok(())
    }

    // --- Manifest ---

    pub(crate) fn get_manifest(&self) -> io::Result<Option<Manifest>> {
        S3Client::block_on(&self.runtime, self.get_manifest_async())
    }

    /// Phase Thermopylae: try msgpack first, fall back to JSON for automigration.
    pub(crate) async fn get_manifest_async(&self) -> io::Result<Option<Manifest>> {
        // Try msgpack first
        let msgpack_key = self.manifest_key_msgpack();
        if let Some(bytes) = self.get_object_async(&msgpack_key).await? {
            let mut manifest: Manifest = rmp_serde::from_slice(&bytes).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Invalid manifest msgpack: {}", e),
                )
            })?;
            manifest.build_page_index();
            return Ok(Some(manifest));
        }
        // Fall back to JSON (pre-Thermopylae manifests)
        let json_key = self.manifest_key_json();
        match self.get_object_async(&json_key).await? {
            Some(bytes) => {
                let mut manifest: Manifest = serde_json::from_slice(&bytes).map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Invalid manifest JSON: {}", e),
                    )
                })?;
                manifest.build_page_index();
                Ok(Some(manifest))
            }
            None => Ok(None),
        }
    }

    pub(crate) fn put_manifest(&self, manifest: &Manifest) -> io::Result<()> {
        S3Client::block_on(&self.runtime, self.put_manifest_async(manifest))
    }

    /// Phase Thermopylae: always write msgpack.
    pub(crate) async fn put_manifest_async(&self, manifest: &Manifest) -> io::Result<()> {
        let key = self.manifest_key_msgpack();
        let data = rmp_serde::to_vec(manifest)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.put_object_async(&key, data, Some("application/msgpack")).await
    }

    // ── Conditional manifest CAS (Iceberg-style commit protocol) ──
    //
    // S3 supports If-Match on PutObject since Nov 2024. Paired with an ETag
    // captured at GET time, this gives us optimistic concurrency on the
    // manifest pointer — the same shape Apache Iceberg uses for its table
    // metadata commit. `put_manifest_conditional_async` returns
    // `ManifestCasError::PreconditionFailed` (not a plain io::Error) when a
    // concurrent writer has committed first, so callers can distinguish CAS
    // conflicts from transient I/O faults and implement retry/rebase loops.
    //
    // Stage A (this commit): primitives only. No call-site migration.

    /// Fetch the manifest along with the S3 ETag for conditional PUT.
    /// Returns `(None, None)` when no manifest exists yet.
    pub(crate) async fn get_manifest_with_etag_async(
        &self,
    ) -> io::Result<(Option<Manifest>, Option<String>)> {
        // Try msgpack first
        let msgpack_key = self.manifest_key_msgpack();
        if let Some((bytes, etag)) = self.get_object_with_etag_async(&msgpack_key).await? {
            let mut manifest: Manifest = rmp_serde::from_slice(&bytes).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Invalid manifest msgpack: {}", e),
                )
            })?;
            manifest.build_page_index();
            return Ok((Some(manifest), Some(etag)));
        }
        // Fall back to JSON (pre-Thermopylae manifests)
        let json_key = self.manifest_key_json();
        match self.get_object_with_etag_async(&json_key).await? {
            Some((bytes, etag)) => {
                let mut manifest: Manifest = serde_json::from_slice(&bytes).map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Invalid manifest JSON: {}", e),
                    )
                })?;
                manifest.build_page_index();
                Ok((Some(manifest), Some(etag)))
            }
            None => Ok((None, None)),
        }
    }

    /// Blocking wrapper for [`get_manifest_with_etag_async`].
    pub(crate) fn get_manifest_with_etag(
        &self,
    ) -> io::Result<(Option<Manifest>, Option<String>)> {
        S3Client::block_on(&self.runtime, self.get_manifest_with_etag_async())
    }

    /// PUT the manifest with an optional `If-Match` precondition.
    ///
    /// When `if_match` is `Some(etag)`, the write succeeds only if the current
    /// object's ETag equals `etag`. A mismatch surfaces as
    /// [`ManifestCasError::PreconditionFailed`] — the caller decides whether
    /// to refetch + rebase + retry.
    ///
    /// When `if_match` is `None`, behaves as an unconditional write (equivalent
    /// to [`put_manifest_async`]). This variant exists so callers migrating
    /// to the CAS API can flip the behavior via one flag rather than maintain
    /// two code paths.
    ///
    /// No internal retry loop: precondition failures are a CAS signal, and
    /// transient I/O failures are the caller's responsibility since a retry
    /// would race against other writers.
    pub(crate) async fn put_manifest_conditional_async(
        &self,
        manifest: &Manifest,
        if_match: Option<&str>,
    ) -> Result<(), ManifestCasError> {
        let key = self.manifest_key_msgpack();
        let data = rmp_serde::to_vec(manifest)
            .map_err(|e| ManifestCasError::Io(io::Error::new(io::ErrorKind::Other, e)))?;
        let data_len = data.len() as u64;
        let body = aws_sdk_s3::primitives::ByteStream::from(data);
        let mut req = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(body)
            .content_type("application/msgpack");
        if let Some(etag) = if_match {
            req = req.if_match(etag);
        }
        match req.send().await {
            Ok(_) => {
                self.put_count.fetch_add(1, Ordering::Relaxed);
                self.put_bytes.fetch_add(data_len, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                if is_precondition_failed(&e) {
                    Err(ManifestCasError::PreconditionFailed)
                } else {
                    Err(ManifestCasError::Io(io::Error::new(
                        io::ErrorKind::Other,
                        format!("S3 PUT {} failed: {}", key, e),
                    )))
                }
            }
        }
    }

    /// Blocking wrapper for [`put_manifest_conditional_async`].
    pub(crate) fn put_manifest_conditional(
        &self,
        manifest: &Manifest,
        if_match: Option<&str>,
    ) -> Result<(), ManifestCasError> {
        S3Client::block_on(
            &self.runtime,
            self.put_manifest_conditional_async(manifest, if_match),
        )
    }

    /// GET that captures the S3 ETag. Mirrors [`get_object_async`]'s retry
    /// semantics (3 retries on transient errors, None on 404).
    async fn get_object_with_etag_async(
        &self,
        key: &str,
    ) -> io::Result<Option<(Vec<u8>, String)>> {
        let mut retries = 0u32;
        loop {
            match self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
                .await
            {
                Ok(resp) => {
                    let etag = resp.e_tag().map(|s| s.to_string()).unwrap_or_default();
                    let bytes = resp
                        .body
                        .collect()
                        .await
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
                        .into_bytes();
                    self.fetch_count.fetch_add(1, Ordering::Relaxed);
                    self.fetch_bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                    return Ok(Some((bytes.to_vec(), etag)));
                }
                Err(e) => {
                    if is_not_found(&e) {
                        return Ok(None);
                    }
                    retries += 1;
                    if retries >= 3 {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!("S3 GET {} failed after 3 retries: {:?}", key, e),
                        ));
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(
                        100 * (1 << retries),
                    ))
                    .await;
                }
            }
        }
    }

    /// Delete a batch of S3 objects by key. Handles batching (AWS limit: 1000/request).
    pub(crate) fn delete_objects(&self, keys: &[String]) -> io::Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        S3Client::block_on(&self.runtime, self.delete_objects_async(keys))
    }

    pub(crate) async fn delete_objects_async(&self, keys: &[String]) -> io::Result<()> {
        for batch in keys.chunks(1000) {
            let objects: Vec<aws_sdk_s3::types::ObjectIdentifier> = batch
                .iter()
                .map(|key| {
                    aws_sdk_s3::types::ObjectIdentifier::builder()
                        .key(key)
                        .build()
                        .expect("ObjectIdentifier requires key")
                })
                .collect();

            let delete = aws_sdk_s3::types::Delete::builder()
                .set_objects(Some(objects))
                .quiet(true)
                .build()
                .map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::Other,
                        format!("Failed to build Delete request: {}", e),
                    )
                })?;

            self.client
                .delete_objects()
                .bucket(&self.bucket)
                .delete(delete)
                .send()
                .await
                .map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::Other,
                        format!("S3 batch delete failed: {}", e),
                    )
                })?;
        }
        Ok(())
    }

    /// Fire-and-forget async delete for background GC. Owns the keys vec.
    /// Logs errors internally instead of returning them.
    pub(crate) async fn delete_objects_async_owned(&self, keys: Vec<String>) {
        let count = keys.len();
        if let Err(e) = self.delete_objects_async(&keys).await {
            eprintln!("[gc] ERROR: background delete of {} objects failed: {}", count, e);
        } else {
            turbolite_debug!("[gc] deleted {} old versions", count);
        }
    }

    /// List all S3 object keys under this client's prefix.
    pub(crate) fn list_all_keys(&self) -> io::Result<Vec<String>> {
        S3Client::block_on(&self.runtime, self.list_all_keys_async())
    }

    pub(crate) async fn list_all_keys_async(&self) -> io::Result<Vec<String>> {
        let mut all_keys = Vec::new();
        let mut continuation_token: Option<String> = None;
        loop {
            let mut req = self.client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&self.prefix);
            if let Some(token) = &continuation_token {
                req = req.continuation_token(token);
            }
            let resp = req.send().await.map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("S3 list failed: {}", e))
            })?;
            for obj in resp.contents() {
                if let Some(key) = obj.key() {
                    all_keys.push(key.to_string());
                }
            }
            if resp.is_truncated() == Some(true) {
                continuation_token = resp.next_continuation_token().map(|s| s.to_string());
            } else {
                break;
            }
        }
        Ok(all_keys)
    }

    // --- Phase Recall: snapshot manifest helpers ---

    /// S3 key for a snapshot manifest copy: `{prefix}/manifest-snap-{snap_id}.msgpack`
    pub(crate) fn snapshot_manifest_key(&self, snap_id: &str) -> String {
        self.s3_key(&format!("manifest-snap-{}.msgpack", snap_id))
    }

    /// Copy the current manifest to a snapshot key. Returns the S3 key of the copy.
    /// Used by the control plane at snapshot time to protect page groups from GC.
    pub(crate) fn copy_manifest_to_snapshot(&self, snap_id: &str) -> io::Result<String> {
        S3Client::block_on(&self.runtime, self.copy_manifest_to_snapshot_async(snap_id))
    }

    pub(crate) async fn copy_manifest_to_snapshot_async(&self, snap_id: &str) -> io::Result<String> {
        let current_key = self.manifest_key_msgpack();
        let snap_key = self.snapshot_manifest_key(snap_id);

        // Read-then-write instead of S3 CopyObject for portability across S3 providers.
        let data = self.get_object_async(&current_key).await?.ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "no manifest.msgpack to snapshot")
        })?;
        self.put_object_async(&snap_key, data, Some("application/msgpack")).await?;
        Ok(snap_key)
    }

    /// Load a manifest from an arbitrary S3 key (e.g. a snapshot manifest copy).
    /// Returns None if the key does not exist.
    /// On deserialization error, returns Err (caller decides whether to skip or propagate).
    pub(crate) fn get_manifest_at_key(&self, key: &str) -> io::Result<Option<Manifest>> {
        S3Client::block_on(&self.runtime, self.get_manifest_at_key_async(key))
    }

    pub(crate) async fn get_manifest_at_key_async(&self, key: &str) -> io::Result<Option<Manifest>> {
        match self.get_object_async(key).await? {
            Some(bytes) => {
                let mut manifest: Manifest = rmp_serde::from_slice(&bytes).map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Invalid snapshot manifest msgpack at {}: {}", key, e),
                    )
                })?;
                manifest.build_page_index();
                Ok(Some(manifest))
            }
            None => Ok(None),
        }
    }

    /// Delete a snapshot manifest copy from S3.
    pub(crate) fn delete_snapshot_manifest(&self, snap_id: &str) -> io::Result<()> {
        let key = self.snapshot_manifest_key(snap_id);
        self.delete_objects(&[key])
    }

    /// List all S3 keys under an arbitrary prefix (not limited to self.prefix).
    #[allow(dead_code)]
    pub(crate) async fn list_all_keys_with_prefix(&self, prefix: &str) -> io::Result<Vec<String>> {
        let mut all_keys = Vec::new();
        let mut continuation_token: Option<String> = None;
        loop {
            let mut req = self.client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix);
            if let Some(token) = &continuation_token {
                req = req.continuation_token(token);
            }
            let resp = req.send().await.map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("S3 list (prefix={}) failed: {}", prefix, e))
            })?;
            for obj in resp.contents() {
                if let Some(key) = obj.key() {
                    all_keys.push(key.to_string());
                }
            }
            if resp.is_truncated() == Some(true) {
                continuation_token = resp.next_continuation_token().map(|s| s.to_string());
            } else {
                break;
            }
        }
        Ok(all_keys)
    }
}

#[cfg(feature = "cloud")]
/// Check if an S3 error is a 404 / NoSuchKey.
pub(crate) fn is_not_found<E: std::fmt::Display + std::fmt::Debug>(
    err: &aws_sdk_s3::error::SdkError<E>,
) -> bool {
    match err {
        aws_sdk_s3::error::SdkError::ServiceError(service_err) => {
            service_err.raw().status().as_u16() == 404
        }
        _ => false,
    }
}

#[cfg(feature = "cloud")]
/// Check if an S3 error is a 412 / PreconditionFailed (If-Match mismatch).
pub(crate) fn is_precondition_failed<E: std::fmt::Display + std::fmt::Debug>(
    err: &aws_sdk_s3::error::SdkError<E>,
) -> bool {
    match err {
        aws_sdk_s3::error::SdkError::ServiceError(service_err) => {
            service_err.raw().status().as_u16() == 412
        }
        _ => false,
    }
}

/// Error from a conditional manifest PUT. Distinct from [`io::Error`] so
/// callers can branch on CAS conflicts vs. general I/O faults.
#[cfg(feature = "cloud")]
#[derive(Debug)]
pub(crate) enum ManifestCasError {
    /// S3 returned 412 Precondition Failed — another writer committed a
    /// newer manifest after we read the one we tried to CAS against. Caller
    /// should refetch the manifest, rebase its in-progress changes, and retry.
    PreconditionFailed,
    /// Any other error (network, serialization, non-412 service errors).
    Io(io::Error),
}

#[cfg(feature = "cloud")]
impl std::fmt::Display for ManifestCasError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ManifestCasError::PreconditionFailed => {
                write!(f, "manifest CAS precondition failed (concurrent writer)")
            }
            ManifestCasError::Io(e) => write!(f, "manifest CAS I/O error: {}", e),
        }
    }
}

#[cfg(feature = "cloud")]
impl std::error::Error for ManifestCasError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ManifestCasError::PreconditionFailed => None,
            ManifestCasError::Io(e) => Some(e),
        }
    }
}

#[cfg(feature = "cloud")]
impl From<io::Error> for ManifestCasError {
    fn from(e: io::Error) -> Self {
        ManifestCasError::Io(e)
    }
}

#[cfg(test)]
#[path = "test_s3_client.rs"]
mod tests;

