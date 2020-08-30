use crate::cache::{Cache, CacheRead, CacheWrite, Storage};
use crate::errors::{f_ok, SFuture};
use anyhow::{Context, Result};
use futures_03::prelude::*;
use mongodb::bson::spec::BinarySubtype;
use mongodb::bson::{self, doc, Binary};
use mongodb::{Client, Collection};
use serde::{Deserialize, Serialize};
use std::io::Cursor;
use std::time::{Duration, Instant};

// Struct which puts into MongoDB
#[derive(Serialize, Deserialize)]
struct CacheEntry {
    key: String,
    cache: Binary,
}

#[derive(Clone)]
pub struct MongoDBCache {
    url: String,
    database_name: String,
    collection_name: String,
}

impl MongoDBCache {
    pub fn new(url: &str, database_name: &str, collection_name: &str) -> Result<MongoDBCache> {
        Ok(Self {
            url: url.into(),
            database_name: database_name.into(),
            collection_name: collection_name.into(),
        })
    }

    pub async fn connect(&self) -> Result<Collection> {
        let collection = Client::with_uri_str(&self.url)
            .await
            .context("failed to create MongoDB client")?
            .database(&self.database_name)
            .collection(&self.collection_name);

        Ok(collection)
    }
}

impl Storage for MongoDBCache {
    fn get(&self, key: &str) -> SFuture<Cache> {
        let me = self.clone();
        let key = key.to_string();

        let fut = async move {
            let data = me
                .connect()
                .await?
                .find_one(doc! { "key": &key }, None)
                .await
                .context("failed to fetch entry")?;

            match data {
                Some(document) => {
                    let entry = bson::from_document::<CacheEntry>(document)
                        .context("failed to deserialize MongoDB entry")?;

                    let reader = CacheRead::from(Cursor::new(entry.cache.bytes))
                        .context("failed to create new cursor")?;

                    log::debug!("cache hit, key: {}", key);
                    Ok(Cache::Hit(reader))
                }

                None => {
                    log::debug!("cache miss, key: {}", key);
                    Ok(Cache::Miss)
                }
            }
        };

        Box::new(Box::pin(fut).compat())
    }

    fn put(&self, key: &str, entry: CacheWrite) -> SFuture<Duration> {
        let me = self.clone();
        let key = key.to_string();
        let start = Instant::now();

        let fut = async move {
            let entry = CacheEntry {
                key,
                cache: Binary {
                    subtype: BinarySubtype::Generic,
                    bytes: entry.finish()?,
                },
            };

            let doc = bson::to_document(&entry).context("failed to serialize cache")?;

            me.connect()
                .await?
                .insert_one(doc, None)
                .await
                .context("failed to insert to MongoDB")?;

            Ok(start.elapsed())
        };

        Box::new(Box::pin(fut).compat())
    }

    fn location(&self) -> String {
        format!(
            "MongoDB: {}, database: {}, collection: {}",
            &self.url, &self.database_name, &self.collection_name
        )
    }

    fn current_size(&self) -> SFuture<Option<u64>> {
        f_ok(None)
    }

    fn max_size(&self) -> SFuture<Option<u64>> {
        f_ok(None)
    }
}
