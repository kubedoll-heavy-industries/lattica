use std::borrow::Cow;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use libp2p::kad::store::{MemoryStore, RecordStore, Error};
use libp2p::kad::{ProviderRecord, Record, RecordKey};
use libp2p::PeerId;
use anyhow::{anyhow, Result};
use bincode::{Decode, Encode};
use tokio::task::JoinHandle;

/// Prefix byte for record entries in the backing store.
const RECORD_PREFIX: u8 = 0x00;
/// Prefix byte for provider record entries in the backing store.
const PROVIDER_PREFIX: u8 = 0x01;

// ---------------------------------------------------------------------------
// Serialization types (backend-agnostic)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Encode, Decode)]
struct StoredRecord {
    key: Vec<u8>,
    value: Vec<u8>,
    publisher: Option<Vec<u8>>,
    expires: Option<u64>,
}

#[derive(Debug, Clone, Encode, Decode)]
struct StoredProviderRecord {
    key: Vec<u8>,
    provider: Vec<u8>,
    expires: Option<u64>,
    addresses: Vec<Vec<u8>>,
}

impl From<Record> for StoredRecord {
    fn from(record: Record) -> Self {
        let expires = record.expires.map(|t| {
            let now = Instant::now();
            let duration = t.saturating_duration_since(now);
            SystemTime::now()
                .checked_add(duration)
                .unwrap_or(SystemTime::now())
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64
        });

        Self {
            key: record.key.as_ref().to_vec(),
            value: record.value.clone(),
            publisher: record.publisher.map(|p| p.to_bytes()),
            expires,
        }
    }
}

impl From<StoredRecord> for Record {
    fn from(stored: StoredRecord) -> Self {
        let mut record = Record::new(
            RecordKey::new(&stored.key),
            stored.value,
        );

        record.publisher = stored.publisher.and_then(|bytes| {
            PeerId::from_bytes(&bytes).ok()
        });

        record.expires = stored.expires.map(|ts| {
            let now_system = SystemTime::now();
            let now_instant = Instant::now();
            let expire_system = UNIX_EPOCH + Duration::from_secs(ts);
            let delta = expire_system.duration_since(now_system).unwrap_or_default();
            now_instant + delta
        });

        record
    }
}

impl From<&ProviderRecord> for StoredProviderRecord {
    fn from(record: &ProviderRecord) -> Self {
        let expires = record.expires.map(|t| {
            let now = Instant::now();
            let duration = t.saturating_duration_since(now);
            SystemTime::now()
                .checked_add(duration)
                .unwrap_or(SystemTime::now())
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64
        });

        Self {
            key: record.key.as_ref().to_vec(),
            provider: record.provider.to_bytes(),
            expires,
            addresses: record.addresses.iter().map(|a| a.to_vec()).collect(),
        }
    }
}

impl TryFrom<StoredProviderRecord> for ProviderRecord {
    type Error = anyhow::Error;

    fn try_from(stored: StoredProviderRecord) -> Result<Self> {
        let provider = PeerId::from_bytes(&stored.provider)
            .map_err(|e| anyhow!("invalid provider PeerId: {}", e))?;

        let addresses: Vec<libp2p::Multiaddr> = stored.addresses
            .into_iter()
            .filter_map(|bytes| libp2p::Multiaddr::try_from(bytes).ok())
            .collect();

        let expires = stored.expires.map(|ts| {
            let now_system = SystemTime::now();
            let now_instant = Instant::now();
            let expire_system = UNIX_EPOCH + Duration::from_secs(ts);
            let delta = expire_system.duration_since(now_system).unwrap_or_default();
            now_instant + delta
        });

        Ok(ProviderRecord {
            key: RecordKey::new(&stored.key),
            provider,
            expires,
            addresses,
        })
    }
}

// ---------------------------------------------------------------------------
// DhtBackend trait
// ---------------------------------------------------------------------------

/// Abstraction over the persistent backing store for DHT records.
///
/// All operations are synchronous (called from the libp2p `RecordStore` trait
/// which is not async). Implementations must be `Send + Sync` so the store
/// can be shared across the cleanup task.
pub trait DhtBackend: Send + Sync {
    /// Retrieve a raw value by its full prefixed key.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Insert a raw key-value pair.
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;

    /// Remove a key.
    fn remove(&self, key: &[u8]) -> Result<()>;

    /// Iterate over all key-value pairs whose key starts with `prefix`.
    fn scan_prefix(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)>;

    /// Iterate over all key-value pairs in the store.
    fn iter_all(&self) -> Vec<(Vec<u8>, Vec<u8>)>;

    /// Flush buffered writes to durable storage.
    fn flush(&self) -> Result<()>;
}

// ---------------------------------------------------------------------------
// Sled backend
// ---------------------------------------------------------------------------

/// Sled-based implementation of [`DhtBackend`].
pub struct SledBackend {
    db: sled::Db,
}

impl SledBackend {
    pub fn open(path: &str) -> Result<Self> {
        let db = sled::open(path)?;
        Ok(Self { db })
    }
}

impl DhtBackend for SledBackend {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.db.get(key)?.map(|v| v.to_vec()))
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.db.insert(key, value)?;
        Ok(())
    }

    fn remove(&self, key: &[u8]) -> Result<()> {
        self.db.remove(key)?;
        Ok(())
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.db.scan_prefix(prefix)
            .filter_map(|r| r.ok())
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect()
    }

    fn iter_all(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.db.iter()
            .filter_map(|r| r.ok())
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect()
    }

    fn flush(&self) -> Result<()> {
        self.db.flush()?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Key construction helpers
// ---------------------------------------------------------------------------

/// Build a store key for a regular DHT record.
fn record_store_key(key: &RecordKey) -> Vec<u8> {
    let mut k = Vec::with_capacity(1 + key.as_ref().len());
    k.push(RECORD_PREFIX);
    k.extend_from_slice(key.as_ref());
    k
}

/// Build a store key for a provider record (unique per key+provider pair).
fn provider_store_key(key: &RecordKey, provider: &PeerId) -> Vec<u8> {
    let provider_bytes = provider.to_bytes();
    let mut k = Vec::with_capacity(1 + key.as_ref().len() + 1 + provider_bytes.len());
    k.push(PROVIDER_PREFIX);
    k.extend_from_slice(key.as_ref());
    k.push(b'/');
    k.extend_from_slice(&provider_bytes);
    k
}

/// Build a store key prefix for all providers of a given record key.
fn provider_store_prefix(key: &RecordKey) -> Vec<u8> {
    let mut k = Vec::with_capacity(1 + key.as_ref().len() + 1);
    k.push(PROVIDER_PREFIX);
    k.extend_from_slice(key.as_ref());
    k.push(b'/');
    k
}

// ---------------------------------------------------------------------------
// MultiStore
// ---------------------------------------------------------------------------

pub struct MultiStore {
    pub memory_store: Arc<RwLock<MemoryStore>>,
    backend: Arc<RwLock<Box<dyn DhtBackend>>>,
    local_peer_id: PeerId,
    _cleanup_handle: Option<JoinHandle<()>>,
}

impl MultiStore {
    /// Create a new `MultiStore` with the default sled backend.
    pub fn new(peer_id: PeerId, db_path: String) -> Result<Self> {
        let backend = SledBackend::open(&db_path)?;
        Self::with_backend(peer_id, Box::new(backend))
    }

    /// Create a new `MultiStore` with a custom [`DhtBackend`].
    pub fn with_backend(peer_id: PeerId, backend: Box<dyn DhtBackend>) -> Result<Self> {
        let memory_store = Arc::new(RwLock::new(MemoryStore::new(peer_id)));
        let backend = Arc::new(RwLock::new(backend));

        let mut store = Self {
            memory_store,
            backend,
            local_peer_id: peer_id,
            _cleanup_handle: None,
        };

        let memory_store_clone = store.memory_store.clone();
        let backend_clone = store.backend.clone();

        let cleanup_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(300));

            loop {
                interval.tick().await;

                if let Err(e) = Self::cleanup(&memory_store_clone, &backend_clone) {
                    tracing::error!("Failed to run store cleanup: {}", e);
                }
                tracing::debug!("Store cleanup finished");
            }
        });

        store._cleanup_handle = Some(cleanup_handle);

        Ok(store)
    }

    pub fn warm_up(&mut self) -> Result<()> {
        let backend = self.backend.read().map_err(|e| anyhow!(e.to_string()))?;
        let mut memory_store = self.memory_store.write().map_err(|e| anyhow!(e.to_string()))?;

        // Warm up regular records
        for (key_bytes, value) in backend.scan_prefix(&[RECORD_PREFIX]) {
            // Verify prefix
            if key_bytes.first() != Some(&RECORD_PREFIX) {
                continue;
            }

            let stored: StoredRecord = bincode::decode_from_slice(&value, bincode::config::standard())
                .map(|(data, _)| data)
                .map_err(|e| anyhow!("failed to decode stored record: {}", e))?;

            let record: Record = stored.into();

            if !Self::is_record_expired(&record) {
                let _ = memory_store.put(record);
            }
        }

        // Warm up provider records
        for (_key_bytes, value) in backend.scan_prefix(&[PROVIDER_PREFIX]) {
            if let Ok((stored, _)) = bincode::decode_from_slice::<StoredProviderRecord, _>(&value, bincode::config::standard()) {
                if let Ok(provider_record) = ProviderRecord::try_from(stored) {
                    if !Self::is_provider_expired(&provider_record) {
                        let _ = memory_store.add_provider(provider_record);
                    }
                }
            }
        }

        Ok(())
    }

    fn is_record_expired(record: &Record) -> bool {
        record.expires.is_some_and(|expires| expires < Instant::now())
    }

    fn is_provider_expired(record: &ProviderRecord) -> bool {
        record.expires.is_some_and(|expires| expires < Instant::now())
    }

    /// Persist a provider record synchronously.
    fn persist_provider(backend: &dyn DhtBackend, record: &ProviderRecord) -> Result<()> {
        let stored = StoredProviderRecord::from(record);
        let serialized = bincode::encode_to_vec(&stored, bincode::config::standard())
            .map_err(|e| anyhow!("failed to serialize provider record: {}", e))?;
        let key = provider_store_key(&record.key, &record.provider);
        backend.put(&key, &serialized)
    }

    /// Load all provider records for a given key from the backend.
    fn load_providers(backend: &dyn DhtBackend, key: &RecordKey) -> Vec<ProviderRecord> {
        let prefix = provider_store_prefix(key);
        backend.scan_prefix(&prefix)
            .into_iter()
            .filter_map(|(_, value)| {
                let (stored, _) = bincode::decode_from_slice::<StoredProviderRecord, _>(&value, bincode::config::standard()).ok()?;
                let record = ProviderRecord::try_from(stored).ok()?;
                if Self::is_provider_expired(&record) {
                    None
                } else {
                    Some(record)
                }
            })
            .collect()
    }

    fn cleanup(
        memory_store: &Arc<RwLock<MemoryStore>>,
        backend: &Arc<RwLock<Box<dyn DhtBackend>>>,
    ) -> Result<()> {
        let now_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_nanos() as u64;

        let now = Instant::now();

        // Phase 1: Collect expired keys from memory store (read lock only)
        let expired_keys = if let Ok(memory_store) = memory_store.read() {
            memory_store.records()
                .filter(|record| record.expires.is_some_and(|expires| expires <= now))
                .map(|record| record.key.clone())
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };

        // Phase 2: Remove expired keys from memory store (write lock, no scan)
        if !expired_keys.is_empty() {
            if let Ok(mut memory_store) = memory_store.write() {
                for key in &expired_keys {
                    memory_store.remove(key);
                }
            }
        }

        // Phase 3: Collect expired keys from backend (read lock only)
        let expired_backend_keys = if let Ok(backend) = backend.read() {
            backend.iter_all()
                .into_iter()
                .filter(|(key, value)| {
                    match key.first() {
                        Some(&RECORD_PREFIX) => {
                            bincode::decode_from_slice::<StoredRecord, _>(value, bincode::config::standard())
                                .ok()
                                .and_then(|(stored, _)| stored.expires)
                                .is_some_and(|expires_nanos| expires_nanos <= now_ts)
                        }
                        Some(&PROVIDER_PREFIX) => {
                            bincode::decode_from_slice::<StoredProviderRecord, _>(value, bincode::config::standard())
                                .ok()
                                .and_then(|(stored, _)| stored.expires)
                                .is_some_and(|expires_nanos| expires_nanos <= now_ts)
                        }
                        _ => false,
                    }
                })
                .map(|(key, _)| key)
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };

        // Phase 4: Remove expired keys from backend (write lock, no scan)
        if !expired_backend_keys.is_empty() {
            if let Ok(backend) = backend.write() {
                for key in expired_backend_keys {
                    let _ = backend.remove(&key);
                }
            }
        }

        Ok(())
    }
}

impl Drop for MultiStore {
    fn drop(&mut self) {
        if let Some(handle) = self._cleanup_handle.take() {
            handle.abort();
        }
        // Flush the backend to ensure all writes are durable
        if let Ok(backend) = self.backend.read() {
            if let Err(e) = backend.flush() {
                tracing::error!("Failed to flush DHT store on drop: {}", e);
            }
        }
    }
}

impl RecordStore for MultiStore {
    type RecordsIter<'a> = Box<dyn Iterator<Item = Cow<'a, Record>> + 'a>;

    type ProvidedIter<'a> = Box<dyn Iterator<Item = Cow<'a, ProviderRecord>> + 'a>;

    fn get(&self, k: &RecordKey) -> Option<Cow<'_, Record>> {
        if let Ok(mut memory_store) = self.memory_store.write() {
            if let Some(record) = memory_store.get(k) {
                let record = record.into_owned();

                if Self::is_record_expired(&record) {
                    memory_store.remove(k);
                    return None;
                }

                return Some(Cow::Owned(record));
            }
        }

        let backend = self.backend.try_read().ok()?;
        let store_key = record_store_key(k);
        if let Some(value) = backend.get(&store_key).ok().flatten() {
            if let Ok((stored, _)) = bincode::decode_from_slice::<StoredRecord, _>(&value, bincode::config::standard()) {
                let record: Record = stored.into();

                if Self::is_record_expired(&record) {
                    drop(backend);
                    if let Ok(write_backend) = self.backend.write() {
                        let _ = write_backend.remove(&store_key);
                    }
                    return None;
                }

                if let Ok(mut memory_store) = self.memory_store.write() {
                    let _ = memory_store.put(record.clone());
                }
                return Some(Cow::Owned(record));
            }
        }
        None
    }

    fn put(&mut self, v: Record) -> Result<(), Error> {
        // Write to memory store
        if let Ok(mut memory_store) = self.memory_store.write() {
            memory_store.put(v.clone())?;
        }

        // Write to backend synchronously for durability
        let stored_record = StoredRecord::from(v.clone());
        let serialized = bincode::encode_to_vec(&stored_record, bincode::config::standard())
            .unwrap_or_else(|_e| Vec::new());

        if let Ok(backend) = self.backend.write() {
            let store_key = record_store_key(&v.key);
            let _ = backend.put(&store_key, &serialized);
        }

        Ok(())
    }

    fn remove(&mut self, k: &RecordKey) {
        if let Ok(mut memory_store) = self.memory_store.write() {
            memory_store.remove(k);
        }

        if let Ok(backend) = self.backend.write() {
            let store_key = record_store_key(k);
            let _ = backend.remove(&store_key);
        }
    }

    fn records(&self) -> Self::RecordsIter<'_> {
        // Collect all non-expired records from memory store
        let memory_records: Vec<Cow<'static, Record>> = if let Ok(memory_store) = self.memory_store.read() {
            memory_store.records()
                .filter_map(|cow| {
                    let record = cow.into_owned();
                    if Self::is_record_expired(&record) {
                        None
                    } else {
                        Some(Cow::Owned(record))
                    }
                })
                .collect()
        } else {
            Vec::new()
        };

        // Collect non-expired records from backend that aren't already in memory
        let persistent_records: Vec<Cow<'static, Record>> = if let Ok(backend) = self.backend.try_read() {
            let memory_keys: std::collections::HashSet<Vec<u8>> = memory_records
                .iter()
                .map(|r| r.key.as_ref().to_vec())
                .collect();

            backend.scan_prefix(&[RECORD_PREFIX])
                .into_iter()
                .filter_map(|(_, value)| {
                    let (stored, _) = bincode::decode_from_slice::<StoredRecord, _>(&value, bincode::config::standard()).ok()?;
                    if memory_keys.contains(&stored.key) {
                        return None;
                    }
                    let record: Record = stored.into();
                    if Self::is_record_expired(&record) {
                        None
                    } else {
                        Some(Cow::Owned(record))
                    }
                })
                .collect()
        } else {
            Vec::new()
        };

        Box::new(memory_records.into_iter().chain(persistent_records.into_iter()))
    }

    fn add_provider(&mut self, record: ProviderRecord) -> Result<(), Error> {
        // Persist to backend first (synchronous)
        if let Ok(backend) = self.backend.write() {
            if let Err(e) = Self::persist_provider(&**backend, &record) {
                tracing::error!("Failed to persist provider record: {}", e);
            }
        }

        // Then add to memory store
        if let Ok(mut memory_store) = self.memory_store.write() {
            let _ = memory_store.add_provider(record);
        }

        Ok(())
    }

    fn providers(&self, key: &RecordKey) -> Vec<ProviderRecord> {
        // Try memory store first
        if let Ok(memory_store) = self.memory_store.read() {
            let memory_providers = memory_store.providers(key);
            if !memory_providers.is_empty() {
                return memory_providers;
            }
        }

        // Fall back to backend
        if let Ok(backend) = self.backend.try_read() {
            let providers = Self::load_providers(&**backend, key);
            if !providers.is_empty() {
                // Warm memory store with what we found
                if let Ok(mut memory_store) = self.memory_store.write() {
                    for p in &providers {
                        let _ = memory_store.add_provider(p.clone());
                    }
                }
                return providers;
            }
        }

        vec![]
    }

    fn provided(&self) -> Self::ProvidedIter<'_> {
        // Memory store's `provided()` returns only records where we are the provider
        if let Ok(memory_store) = self.memory_store.read() {
            let vec: Vec<ProviderRecord> = memory_store.provided().map(|c| c.into_owned()).collect();

            if !vec.is_empty() {
                return Box::new(vec.into_iter().map(Cow::Owned));
            }
        }

        // Fall back to backend: scan for provider records where we are the provider
        let local_peer_id = self.local_peer_id;
        if let Ok(backend) = self.backend.try_read() {
            let vec: Vec<ProviderRecord> = backend.scan_prefix(&[PROVIDER_PREFIX])
                .into_iter()
                .filter_map(|(_, value)| {
                    let (stored, _) = bincode::decode_from_slice::<StoredProviderRecord, _>(&value, bincode::config::standard()).ok()?;
                    let record = ProviderRecord::try_from(stored).ok()?;
                    if record.provider == local_peer_id && !Self::is_provider_expired(&record) {
                        Some(record)
                    } else {
                        None
                    }
                })
                .collect();

            // Warm memory store
            if !vec.is_empty() {
                if let Ok(mut memory_store) = self.memory_store.write() {
                    for p in &vec {
                        let _ = memory_store.add_provider(p.clone());
                    }
                }
            }

            return Box::new(vec.into_iter().map(Cow::Owned));
        }

        Box::new(std::iter::empty())
    }

    fn remove_provider(&mut self, k: &RecordKey, p: &PeerId) {
        if let Ok(mut memory_store) = self.memory_store.write() {
            memory_store.remove_provider(k, p);
        }

        if let Ok(backend) = self.backend.write() {
            let key = provider_store_key(k, p);
            let _ = backend.remove(&key);
        }
    }
}
