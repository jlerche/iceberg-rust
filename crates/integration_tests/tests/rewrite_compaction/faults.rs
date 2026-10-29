// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use iceberg::io::{
    CancellationToken, FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage,
    StorageConfig, StorageFactory,
};
use iceberg::table::Table;
use iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit, TableCreation,
    TableIdent,
};
use iceberg_catalog_rest::RestCatalog;
use iceberg_storage_opendal::OpenDalStorageFactory;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Fault {
    #[default]
    None,
    PartialData,
    MetadataClose,
    CancelData,
}

#[derive(Clone, Debug, Default)]
pub struct FaultControl {
    mode: Arc<Mutex<Fault>>,
    pub cancellation: CancellationToken,
}

impl FaultControl {
    pub fn arm(&self, mode: Fault) {
        *self.mode.lock().unwrap_or_else(PoisonError::into_inner) = mode;
    }
    fn take(&self, mode: Fault) -> bool {
        let mut current = self.mode.lock().unwrap_or_else(PoisonError::into_inner);
        if *current != mode {
            return false;
        }
        *current = Fault::None;
        true
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FaultStorageFactory {
    pub inner: OpenDalStorageFactory,
    #[serde(skip)]
    pub faults: FaultControl,
}

#[typetag::serde(name = "compaction-test-factory")]
impl StorageFactory for FaultStorageFactory {
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        Ok(Arc::new(FaultStorage {
            inner: self.inner.build(config)?,
            faults: self.faults.clone(),
        }))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FaultStorage {
    inner: Arc<dyn Storage>,
    #[serde(skip)]
    faults: FaultControl,
}

#[async_trait]
#[typetag::serde(name = "compaction-test-storage")]
impl Storage for FaultStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        self.inner.exists(path).await
    }
    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        self.inner.metadata(path).await
    }
    async fn read(&self, path: &str) -> Result<Bytes> {
        self.inner.read(path).await
    }
    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        self.inner.reader(path).await
    }
    async fn write(&self, path: &str, bytes: Bytes) -> Result<()> {
        self.inner.write(path, bytes).await
    }
    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        Ok(Box::new(FaultWriter {
            inner: self.inner.writer(path).await?,
            storage: Arc::clone(&self.inner),
            path: path.to_string(),
            faults: self.faults.clone(),
        }))
    }
    async fn delete(&self, path: &str) -> Result<()> {
        self.inner.delete(path).await
    }
    async fn delete_prefix(&self, path: &str) -> Result<()> {
        self.inner.delete_prefix(path).await
    }
    async fn delete_stream(&self, paths: BoxStream<'static, String>) -> Result<()> {
        self.inner.delete_stream(paths).await
    }
    fn new_input(&self, path: &str) -> Result<InputFile> {
        self.inner.new_input(path)
    }
    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }
}

struct FaultWriter {
    inner: Box<dyn FileWrite>,
    storage: Arc<dyn Storage>,
    path: String,
    faults: FaultControl,
}

#[async_trait]
impl FileWrite for FaultWriter {
    async fn write(&mut self, bytes: Bytes) -> Result<()> {
        if std::path::Path::new(&self.path)
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("parquet"))
            && self.faults.take(Fault::PartialData)
        {
            let prefix = bytes.slice(..bytes.len().min(16));
            self.storage.write(&self.path, prefix).await?;
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Injected failure after partial S3 PUT",
            ));
        }
        self.inner.write(bytes).await?;
        if std::path::Path::new(&self.path)
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("parquet"))
            && self.faults.take(Fault::CancelData)
        {
            self.faults.cancellation.cancel();
        }
        Ok(())
    }
    async fn close(&mut self) -> Result<FileMetadata> {
        let metadata = self.inner.close().await?;
        if std::path::Path::new(&self.path)
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("avro"))
            && self.faults.take(Fault::MetadataClose)
        {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Injected failure after manifest close",
            ));
        }
        Ok(metadata)
    }
}

#[derive(Debug)]
pub struct LostResponse {
    pub catalog: Arc<RestCatalog>,
    pub updates: AtomicUsize,
    pub cancellation: CancellationToken,
}

#[async_trait]
impl Catalog for LostResponse {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        self.catalog.list_namespaces(parent).await
    }
    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        self.catalog.create_namespace(namespace, properties).await
    }
    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        self.catalog.get_namespace(namespace).await
    }
    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool> {
        self.catalog.namespace_exists(namespace).await
    }
    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        self.catalog.update_namespace(namespace, properties).await
    }
    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        self.catalog.drop_namespace(namespace).await
    }
    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        self.catalog.list_tables(namespace).await
    }
    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        self.catalog.create_table(namespace, creation).await
    }
    async fn load_table(&self, table: &TableIdent) -> Result<Table> {
        self.catalog.load_table(table).await
    }
    async fn drop_table(&self, table: &TableIdent) -> Result<()> {
        self.catalog.drop_table(table).await
    }
    async fn purge_table(&self, table: &TableIdent) -> Result<()> {
        self.catalog.purge_table(table).await
    }
    async fn table_exists(&self, table: &TableIdent) -> Result<bool> {
        self.catalog.table_exists(table).await
    }
    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> Result<()> {
        self.catalog.rename_table(src, dest).await
    }
    async fn register_table(&self, table: &TableIdent, location: String) -> Result<Table> {
        self.catalog.register_table(table, location).await
    }
    async fn update_table(&self, commit: TableCommit) -> Result<Table> {
        self.updates.fetch_add(1, Ordering::SeqCst);
        self.catalog.update_table(commit).await?;
        self.cancellation.cancel();
        Err(Error::new(
            ErrorKind::Unexpected,
            "Injected response loss after REST success",
        )
        .with_retryable(true))
    }
}
