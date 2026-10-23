// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Envelope encryption with a key management service (KMS), like Parquet's key tools.
//!
//! Master keys never leave the KMS. Every [`EnvelopeEncryption::key_config`] generates a random
//! data encryption key (DEK) and stores it, wrapped with a master key, as the key metadata of
//! the file. The [`EnvelopeEncryption`] is also the [`KeyRetriever`] that unwraps it again.
//!
//! With double wrapping, the default, DEKs are wrapped locally with a key encryption key (KEK),
//! and only the KEK is wrapped by the KMS. KEKs are cached per master key when writing, and by
//! id when reading, so that the KMS is called once per master key rather than once per file.
//!
//! Key material is stored in the file by default. With an external [`KeyMaterialStore`], the
//! file only references it, so that [`EnvelopeEncryption::rotate_master_keys`] can rewrap every
//! DEK with the current version of its master key without rewriting files.
//!
//! ```
//! # use std::sync::Arc;
//! # use vortex_array::dtype::FieldPath;
//! # use vortex_error::VortexResult;
//! # use vortex_file::encryption::{FileDecryption, FileEncryption};
//! # use vortex_file::encryption::envelope::{EnvelopeEncryption, KmsClient};
//! # fn example(kms: Arc<dyn KmsClient>) -> VortexResult<()> {
//! let envelope = Arc::new(EnvelopeEncryption::new(kms));
//! let encryption = FileEncryption::new(envelope.key_config("footer-master-key")?)
//!     .with_field_key(FieldPath::from_name("ssn"), "pii", envelope.key_config("pii-master-key")?);
//! let decryption = FileDecryption::new(envelope);
//! # Ok(())
//! # }
//! ```

use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use flatbuffers::FlatBufferBuilder;
use flatbuffers::WIPOffset;
use parking_lot::Mutex;
use parking_lot::RwLock;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_err;
use vortex_utils::aliases::hash_map::HashMap;
use zeroize::Zeroize;

use super::Aead;
use super::EncryptionKey;
use super::KeyConfig;
use super::KeyRetriever;
use super::fill_random;
use crate::flatbuffers::key_material as fb;

const KEK_LEN: usize = 32;
const KEK_ID_LEN: usize = 16;
const EXTERNAL_ID_LEN: usize = 16;

/// A client of a key management service that holds master keys.
///
/// Implementations wrap and unwrap keys with a remote service, such as AWS KMS, Google Cloud KMS,
/// or HashiCorp Vault, and should support master key rotation by unwrapping keys wrapped with any
/// version of a master key.
pub trait KmsClient: Send + Sync {
    /// Wrap `key` with the current version of the master key `master_key_id`.
    fn wrap_key(&self, key: &[u8], master_key_id: &str) -> VortexResult<Vec<u8>>;

    /// Unwrap a key wrapped with any version of the master key `master_key_id`.
    fn unwrap_key(&self, wrapped_key: &[u8], master_key_id: &str) -> VortexResult<Vec<u8>>;
}

/// Stores key material outside of the files it belongs to, like Parquet's external key material.
pub trait KeyMaterialStore: Send + Sync {
    /// Store, or replace, the key material with `id`.
    fn put(&self, id: &str, material: &[u8]) -> VortexResult<()>;

    /// Return the key material with `id`.
    fn get(&self, id: &str) -> VortexResult<Vec<u8>>;

    /// Return the ids of all stored key material.
    fn ids(&self) -> VortexResult<Vec<String>>;
}

/// A [`KeyMaterialStore`] in memory.
#[derive(Default)]
pub struct InMemoryKeyMaterialStore {
    materials: RwLock<HashMap<String, Vec<u8>>>,
}

impl KeyMaterialStore for InMemoryKeyMaterialStore {
    fn put(&self, id: &str, material: &[u8]) -> VortexResult<()> {
        self.materials
            .write()
            .insert(id.to_string(), material.to_vec());
        Ok(())
    }

    fn get(&self, id: &str) -> VortexResult<Vec<u8>> {
        self.materials
            .read()
            .get(id)
            .cloned()
            .ok_or_else(|| vortex_err!("No key material with id {id}"))
    }

    fn ids(&self) -> VortexResult<Vec<String>> {
        Ok(self.materials.read().keys().cloned().collect())
    }
}

/// A key encryption key used to wrap data keys locally.
struct Kek {
    id: Vec<u8>,
    key: EncryptionKey,
    /// The KEK wrapped by the KMS, when writing.
    wrapped: Vec<u8>,
}

/// Generates data keys wrapped by a [`KmsClient`], and unwraps them as a [`KeyRetriever`].
pub struct EnvelopeEncryption {
    kms: Arc<dyn KmsClient>,
    double_wrapping: bool,
    data_key_length: usize,
    store: Option<Arc<dyn KeyMaterialStore>>,
    /// KEKs used to wrap new data keys, by master key id.
    write_keks: Mutex<HashMap<String, Arc<Kek>>>,
    /// Unwrapped KEKs, by KEK id.
    read_keks: Mutex<HashMap<Vec<u8>, EncryptionKey>>,
}

impl EnvelopeEncryption {
    /// Wrap keys with master keys held by `kms`, with double wrapping and 128-bit data keys by
    /// default, as in Parquet.
    pub fn new(kms: Arc<dyn KmsClient>) -> Self {
        Self {
            kms,
            double_wrapping: true,
            data_key_length: 16,
            store: None,
            write_keks: Mutex::default(),
            read_keks: Mutex::default(),
        }
    }

    /// Wrap every data key with the KMS directly, rather than with a cached key encryption key.
    ///
    /// This calls the KMS once for every key written and read.
    pub fn with_single_wrapping(mut self) -> Self {
        self.double_wrapping = false;
        self
    }

    /// Set the length in bytes of generated data keys: 16, 24, or 32.
    pub fn with_data_key_length(mut self, data_key_length: usize) -> Self {
        self.data_key_length = data_key_length;
        self
    }

    /// Keep key material in `store`, and only reference it from files.
    ///
    /// Readers must use an [`EnvelopeEncryption`] with the same store.
    pub fn with_external_key_material(mut self, store: Arc<dyn KeyMaterialStore>) -> Self {
        self.store = Some(store);
        self
    }

    /// Generate a fresh data key wrapped with the master key `master_key_id`.
    ///
    /// Use a fresh key for every file, so that no key encrypts more data than AES-GCM's random
    /// nonces allow.
    pub fn key_config(&self, master_key_id: &str) -> VortexResult<KeyConfig> {
        let dek = EncryptionKey::generate(self.data_key_length)?;
        let mut fbb = FlatBufferBuilder::new();
        let args = match &self.store {
            None => fb::EnvelopeKeyMetadataArgs {
                material: Some(self.write_material(&mut fbb, &dek, master_key_id)?),
                external_id: None,
            },
            Some(store) => {
                let mut id = [0u8; EXTERNAL_ID_LEN];
                fill_random(&mut id)?;
                let id = hex(&id);
                store.put(&id, &self.wrap(&dek, master_key_id)?)?;
                fb::EnvelopeKeyMetadataArgs {
                    material: None,
                    external_id: Some(fbb.create_string(&id)),
                }
            }
        };
        let metadata = fb::EnvelopeKeyMetadata::create(&mut fbb, &args);
        fbb.finish_minimal(metadata);
        Ok(KeyConfig::new(dek, fbb.finished_data()))
    }

    /// Rewrap every data key in the external key material store with the current version of its
    /// master key, returning the number of keys rewrapped.
    ///
    /// The [`KmsClient`] must be able to unwrap keys wrapped with earlier master key versions.
    /// Files keep referencing the same key material, so they stay readable.
    pub fn rotate_master_keys(&self) -> VortexResult<usize> {
        let Some(store) = &self.store else {
            vortex_bail!(
                "Master key rotation requires external key material, configured with \
                 EnvelopeEncryption::with_external_key_material"
            );
        };
        // New KEKs, wrapped with the current master key versions, replace the cached ones.
        self.write_keks.lock().clear();
        let ids = store.ids()?;
        for id in &ids {
            let material = store.get(id)?;
            let (master_key_id, dek) = {
                let material = parse_key_material(&material)?;
                (
                    material.master_key_id().to_string(),
                    self.unwrap(&material)?,
                )
            };
            store.put(id, &self.wrap(&dek, &master_key_id)?)?;
        }
        Ok(ids.len())
    }

    /// Drop the cached key encryption keys, e.g. when KMS access tokens are revoked.
    pub fn clear_caches(&self) {
        self.write_keks.lock().clear();
        self.read_keks.lock().clear();
    }

    /// Wrap `dek` into serialized key material.
    fn wrap(&self, dek: &EncryptionKey, master_key_id: &str) -> VortexResult<Vec<u8>> {
        let mut fbb = FlatBufferBuilder::new();
        let material = self.write_material(&mut fbb, dek, master_key_id)?;
        fbb.finish_minimal(material);
        Ok(fbb.finished_data().to_vec())
    }

    /// Wrap `dek` into key material written to `fbb`.
    fn write_material<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
        dek: &EncryptionKey,
        master_key_id: &str,
    ) -> VortexResult<WIPOffset<fb::KeyMaterial<'fb>>> {
        let args = if self.double_wrapping {
            let kek = self.write_kek(master_key_id)?;
            let mut wrapped_dek = dek.as_bytes().to_vec();
            let nonce_tag = Aead::new(&kek.key)?.seal(&kek.id, &mut wrapped_dek)?;
            wrapped_dek.extend_from_slice(&nonce_tag);
            fb::KeyMaterialArgs {
                master_key_id: Some(fbb.create_string(master_key_id)),
                wrapped_dek: Some(fbb.create_vector(&wrapped_dek)),
                kek_id: Some(fbb.create_vector(&kek.id)),
                wrapped_kek: Some(fbb.create_vector(&kek.wrapped)),
            }
        } else {
            let wrapped_dek = self.kms.wrap_key(dek.as_bytes(), master_key_id)?;
            fb::KeyMaterialArgs {
                master_key_id: Some(fbb.create_string(master_key_id)),
                wrapped_dek: Some(fbb.create_vector(&wrapped_dek)),
                kek_id: None,
                wrapped_kek: None,
            }
        };
        Ok(fb::KeyMaterial::create(fbb, &args))
    }

    /// The KEK for new data keys wrapped with `master_key_id`, created on first use.
    fn write_kek(&self, master_key_id: &str) -> VortexResult<Arc<Kek>> {
        if let Some(kek) = self.write_keks.lock().get(master_key_id) {
            return Ok(Arc::clone(kek));
        }
        let key = EncryptionKey::generate(KEK_LEN)?;
        let mut id = vec![0u8; KEK_ID_LEN];
        fill_random(&mut id)?;
        let wrapped = self.kms.wrap_key(key.as_bytes(), master_key_id)?;
        self.read_keks.lock().insert(id.clone(), key.clone());
        let kek = Arc::new(Kek { id, key, wrapped });
        Ok(Arc::clone(
            self.write_keks
                .lock()
                .entry(master_key_id.to_string())
                .or_insert(kek),
        ))
    }

    /// Unwrap the data key of `material`.
    fn unwrap(&self, material: &fb::KeyMaterial<'_>) -> VortexResult<EncryptionKey> {
        let master_key_id = material.master_key_id();
        let wrapped_dek = material.wrapped_dek().bytes();
        let (Some(kek_id), Some(wrapped_kek)) = (material.kek_id(), material.wrapped_kek()) else {
            let mut dek = self.kms.unwrap_key(wrapped_dek, master_key_id)?;
            let key = EncryptionKey::try_from_slice(&dek);
            dek.zeroize();
            return key;
        };
        let kek_id = kek_id.bytes();
        let cached = self.read_keks.lock().get(kek_id).cloned();
        let kek = match cached {
            Some(kek) => kek,
            None => {
                let mut kek = self.kms.unwrap_key(wrapped_kek.bytes(), master_key_id)?;
                let key = EncryptionKey::try_from_slice(&kek);
                kek.zeroize();
                let key = key?;
                self.read_keks.lock().insert(kek_id.to_vec(), key.clone());
                key
            }
        };
        let Some(len) = wrapped_dek.len().checked_sub(super::SIGNATURE_LEN) else {
            vortex_bail!(
                "Wrapped data key of {} bytes is too short",
                wrapped_dek.len()
            );
        };
        let mut dek = wrapped_dek[..len].to_vec();
        let opened = Aead::new(&kek)?.open(kek_id, &mut dek, &wrapped_dek[len..]);
        let key = opened
            .map_err(|()| vortex_err!("Failed to unwrap the data key with its key encryption key"))
            .and_then(|()| EncryptionKey::try_from_slice(&dek));
        dek.zeroize();
        key
    }
}

impl KeyRetriever for EnvelopeEncryption {
    fn retrieve_key(&self, key_metadata: &[u8]) -> VortexResult<EncryptionKey> {
        let metadata = flatbuffers::root::<fb::EnvelopeKeyMetadata>(key_metadata)
            .map_err(|e| vortex_err!("Invalid envelope key metadata: {e}"))?;
        match (metadata.material(), metadata.external_id()) {
            (Some(material), None) => self.unwrap(&material),
            (None, Some(id)) => {
                let Some(store) = &self.store else {
                    vortex_bail!(
                        "The key material of the file is external, but no key material store was \
                         configured with EnvelopeEncryption::with_external_key_material"
                    );
                };
                self.unwrap(&parse_key_material(&store.get(id)?)?)
            }
            _ => vortex_bail!("Envelope key metadata must hold either key material or its id"),
        }
    }
}

impl Debug for EnvelopeEncryption {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("EnvelopeEncryption")
            .field("double_wrapping", &self.double_wrapping)
            .field("data_key_length", &self.data_key_length)
            .field("external_key_material", &self.store.is_some())
            .finish_non_exhaustive()
    }
}

fn parse_key_material(bytes: &[u8]) -> VortexResult<fb::KeyMaterial<'_>> {
    flatbuffers::root::<fb::KeyMaterial>(bytes)
        .map_err(|e| vortex_err!("Invalid key material: {e}"))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
