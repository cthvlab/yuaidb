use std::collections::HashMap;
use std::sync::Arc;
use dashmap::DashMap;
use serde::{Serialize, Deserialize};
use tokio::fs::{File, create_dir_all};
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use tokio::time::{sleep, Duration};
use toml;
use tokio::task;

#[derive(Debug, Deserialize)]
struct FieldConfig {
    name: String,
    indexed: Option<bool>,
    fulltext: Option<bool>,
}

#[derive(Debug, Deserialize, Default)]
struct TableConfig {
    name: String,
    fields: Vec<FieldConfig>,
}

#[derive(Debug, Deserialize, Default)]
struct DbConfig {
    tables: Vec<TableConfig>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Row {
    pub id: i32,
    pub data: HashMap<String, String>,
}

#[derive(Debug)]
pub enum Condition {
    Eq(String, String),
    Lt(String, String),
    Gt(String, String),
    Contains(String, String),
}

#[derive(Debug)]
pub struct OrderBy {
    pub field: String,
    pub ascending: bool,
}

#[derive(Debug)]
pub struct GroupBy {
    pub field: String,
    pub aggregate: Aggregate,
}

#[derive(Debug)]
pub enum Aggregate {
    Count,
    Sum(String),
}

#[derive(Clone)]
pub struct Database {
    tables: Arc<DashMap<String, Arc<DashMap<i32, Row>>>>,
    indexes: Arc<DashMap<String, Arc<DashMap<String, Arc<DashMap<String, Vec<i32>>>>>>>,
    fulltext_indexes: Arc<DashMap<String, Arc<DashMap<String, Arc<DashMap<String, Vec<i32>>>>>>>,
    data_dir: String,
    config_file: String,
    join_cache: Arc<DashMap<String, Vec<(Row, Row)>>>,
}

impl Database {
    pub async fn new(data_dir: &str, config_file: &str) -> Self {
        let db = Database {
            tables: Arc::new(DashMap::new()),
            indexes: Arc::new(DashMap::new()),
            fulltext_indexes: Arc::new(DashMap::new()),
            data_dir: data_dir.to_string(),
            config_file: config_file.to_string(),
            join_cache: Arc::new(DashMap::new()),
        };
        create_dir_all(data_dir).await.unwrap_or(());
        db.load_tables_from_disk().await;
        let db_clone = db.clone();
        tokio::spawn(async move { db_clone.watch_config().await });
        db
    }

    async fn load_tables_from_disk(&self) {
        if let Ok(mut entries) = tokio::fs::read_dir(&self.data_dir).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                if entry.path().extension().map_or(false, |ext| ext == "bin") {
                    let table_name = entry.path().file_stem().unwrap().to_str().unwrap().to_string();
                    let mut file = File::open(&entry.path()).await.unwrap();
                    let mut buffer = Vec::new();
                    file.read_to_end(&mut buffer).await.unwrap();
                    if let Ok(rows) = bincode::deserialize::<HashMap<i32, Row>>(&buffer) {
                        let table = Arc::new(DashMap::from_iter(rows));
                        self.tables.insert(table_name.clone(), table);
                        self.rebuild_indexes(&table_name).await;
                    }
                }
            }
        }
    }

    async fn save_table(&self, table_name: &str, table: &DashMap<i32, Row>) {
        let path = format!("{}/{}.bin", self.data_dir, table_name);
        let rows: HashMap<i32, Row> = table.iter().map(|r| (*r.key(), r.value().clone())).collect();
        let encoded = bincode::serialize(&rows).unwrap();
        let mut file = File::create(&path).await.unwrap();
        file.write_all(&encoded).await.unwrap();
        file.flush().await.unwrap();
    }

    async fn rebuild_indexes(&self, table_name: &str) {
        if let Some(table) = self.tables.get(table_name) {
            let config: DbConfig = toml::from_str(&tokio::fs::read_to_string(&self.config_file).await.unwrap_or_default()).unwrap_or_default();
            if let Some(table_config) = config.tables.into_iter().find(|t| t.name == table_name) {
                for field in table_config.fields {
                    let (is_indexed, is_fulltext) = (field.indexed.unwrap_or(false), field.fulltext.unwrap_or(false));
                    if is_indexed || is_fulltext {
                        let index = DashMap::new();
                        for row in table.iter() {
                            if let Some(value) = row.data.get(&field.name) {
                                if is_indexed {
                                    index.entry(value.clone()).or_insert_with(Vec::new).push(row.id);
                                    self.indexes.entry(table_name.to_string())
                                        .or_insert_with(|| Arc::new(DashMap::new()))
                                        .insert(field.name.clone(), Arc::new(index.clone()));
                                }
                                if is_fulltext {
                                    for word in value.split_whitespace() {
                                        index.entry(word.to_lowercase()).or_insert_with(Vec::new).push(row.id);
                                    }
                                    self.fulltext_indexes.entry(table_name.to_string())
                                        .or_insert_with(|| Arc::new(DashMap::new()))
                                        .insert(field.name.clone(), Arc::new(index.clone()));
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    async fn watch_config(&self) {
        let mut last_content = String::new();
        loop {
            if let Ok(content) = tokio::fs::read_to_string(&self.config_file).await {
                if content != last_content {
                    println!("Обновление конфигурации...");
                    self.apply_config().await;
                    last_content = content;
                }
            }
            sleep(Duration::from_secs(5)).await;
        }
    }

    async fn apply_config(&self) {
        let config: DbConfig = toml::from_str(&tokio::fs::read_to_string(&self.config_file).await.unwrap_or_default()).unwrap_or_default();
        for table_config in config.tables {
            let table_name = table_config.name;
            if !self.tables.contains_key(&table_name) {
                let table = Arc::new(DashMap::new());
                self.tables.insert(table_name.clone(), table.clone());
                self.save_table(&table_name, &table).await;
            }
            self.rebuild_indexes(&table_name).await;
        }
    }

    async fn update_indexes(&self, table_name: &str, row: &Row, remove: bool) {
        let mut tasks = Vec::new();
        let table_name_owned = table_name.to_string();

        for (field, value) in &row.data {
            let field_owned = field.clone();
            let value_owned = value.clone();

            if self.indexes.contains_key(&table_name_owned) && self.indexes.get(&table_name_owned).unwrap().contains_key(field) {
                let indexes = self.indexes.clone();
                let table_name_clone = table_name_owned.clone();
                let field_clone = field_owned.clone();
                let value_clone = value_owned.clone();
                let row_id = row.id;
                tasks.push(task::spawn(async move {
                    let index_ref = indexes.get(&table_name_clone).unwrap();
                    let index = index_ref.get_mut(&field_clone).unwrap();
                    if remove {
                        if let Some(mut ids) = index.get_mut(&value_clone) {
                            ids.retain(|&x| x != row_id);
                        }
                    } else {
                        index.entry(value_clone).or_insert_with(Vec::new).push(row_id);
                    }
                }));
            }

            if self.fulltext_indexes.contains_key(&table_name_owned) && self.fulltext_indexes.get(&table_name_owned).unwrap().contains_key(field) {
                let fulltext_indexes = self.fulltext_indexes.clone();
                let table_name_clone = table_name_owned.clone();
                let field_clone = field_owned.clone();
                let value_clone = value_owned.clone();
                let row_id = row.id;
                tasks.push(task::spawn(async move {
                    let ft_index_ref = fulltext_indexes.get(&table_name_clone).unwrap();
                    let ft_index = ft_index_ref.get_mut(&field_clone).unwrap();
                    for word in value_clone.split_whitespace() {
                        let word = word.to_lowercase();
                        if remove {
                            if let Some(mut ids) = ft_index.get_mut(&word) {
                                ids.retain(|&x| x != row_id);
                            }
                        } else {
                            ft_index.entry(word).or_insert_with(Vec::new).push(row_id);
                        }
                    }
                }));
            }
        }

        for task in tasks {
            task.await.unwrap();
        }
    }

    pub async fn create_table(&self, name: &str) {
        if !self.tables.contains_key(name) {
            let table = Arc::new(DashMap::new());
            self.tables.insert(name.to_string(), table.clone());
            self.save_table(name, &table).await;
        }
    }

    pub async fn insert(&self, table_name: &str, row: Row) {
        if let Some(table) = self.tables.get(table_name) {
            table.insert(row.id, row.clone());
            self.update_indexes(table_name, &row, false).await;
            self.save_table(table_name, &table).await;
            self.join_cache.retain(|key, _| !key.contains(table_name));
        }
    }

    pub async fn update(&self, table_name: &str, conditions: Vec<Condition>, updates: HashMap<String, String>) {
        if let Some(table) = self.tables.get(table_name) {
            let mut to_update = table.iter().map(|r| r.value().clone()).collect::<Vec<_>>();
            for condition in conditions {
                to_update = self.filter_rows(table_name, &table, to_update, condition);
            }
            for mut row in to_update {
                self.update_indexes(table_name, &row, true).await;
                row.data.extend(updates.clone());
                self.update_indexes(table_name, &row, false).await;
                table.insert(row.id, row);
            }
            self.save_table(table_name, &table).await;
            self.join_cache.retain(|key, _| !key.contains(table_name));
        }
    }

    pub async fn delete(&self, table_name: &str, conditions: Vec<Condition>) {
        if let Some(table) = self.tables.get(table_name) {
            let mut to_delete = table.iter().map(|r| r.value().clone()).collect::<Vec<_>>();
            for condition in conditions {
                to_delete = self.filter_rows(table_name, &table, to_delete, condition);
            }
            for row in to_delete {
                self.update_indexes(table_name, &row, true).await;
                table.remove(&row.id);
            }
            self.save_table(table_name, &table).await;
            self.join_cache.retain(|key, _| !key.contains(table_name));
        }
    }

    fn filter_rows(&self, table_name: &str, table: &DashMap<i32, Row>, rows: Vec<Row>, condition: Condition) -> Vec<Row> {
        match condition {
            Condition::Eq(field, value) => {
                if let Some(index_ref) = self.indexes.get(table_name) {
                    if let Some(index) = index_ref.get(&field) {
                        index.get(&value).map_or(vec![], |ids| {
                            ids.iter().filter_map(|id| table.get(id).map(|r| r.value().clone())).collect()
                        })
                    } else {
                        rows.into_iter().filter(|row| row.data.get(&field).map_or(false, |v| v == &value)).collect()
                    }
                } else {
                    rows.into_iter().filter(|row| row.data.get(&field).map_or(false, |v| v == &value)).collect()
                }
            }
            Condition::Lt(field, value) => rows.into_iter().filter(|row| row.data.get(&field).map_or(false, |v| v < &value)).collect(),
            Condition::Gt(field, value) => rows.into_iter().filter(|row| row.data.get(&field).map_or(false, |v| v > &value)).collect(),
            Condition::Contains(field, value) => {
                if let Some(ft_index_ref) = self.fulltext_indexes.get(table_name) {
                    if let Some(ft_index) = ft_index_ref.get(&field) {
                        let value_lower = value.to_lowercase();
                        let mut ids: Vec<i32> = Vec::new();
                        for entry in ft_index.iter() {
                            if entry.key().contains(&value_lower) {
                                ids.extend(entry.value().clone());
                            }
                        }
                        ids.sort_unstable();
                        ids.dedup();
                        ids.into_iter().filter_map(|id| table.get(&id).map(|r| r.value().clone())).collect()
                    } else {
                        rows.into_iter().filter(|row| row.data.get(&field).map_or(false, |v| v.to_lowercase().contains(&value.to_lowercase()))).collect()
                    }
                } else {
                    rows.into_iter().filter(|row| row.data.get(&field).map_or(false, |v| v.to_lowercase().contains(&value.to_lowercase()))).collect()
                }
            }
        }
    }

    pub async fn delete_table(&self, table_name: &str) {
        if self.tables.remove(table_name).is_some() {
            self.indexes.remove(table_name);
            self.fulltext_indexes.remove(table_name);
            tokio::fs::remove_file(format!("{}/{}.bin", self.data_dir, table_name)).await.unwrap_or(());
            self.join_cache.retain(|key, _| !key.contains(table_name));
        }
    }

    pub async fn create_index(&self, table_name: &str, field: &str) {
        if let Some(table) = self.tables.get(table_name) {
            if !self.indexes.get(table_name).map_or(false, |idx| idx.contains_key(field)) {
                let index = DashMap::new();
                for row in table.iter() {
                    if let Some(value) = row.data.get(field) {
                        index.entry(value.clone()).or_insert_with(Vec::new).push(row.id);
                    }
                }
                self.indexes.entry(table_name.to_string())
                    .or_insert_with(|| Arc::new(DashMap::new()))
                    .insert(field.to_string(), Arc::new(index));
                self.save_table(table_name, &table).await;
                self.join_cache.retain(|key, _| !key.contains(table_name));
            }
        }
    }

    pub async fn select(&self, table_name: &str, conditions: Vec<Condition>, order_by: Option<OrderBy>, group_by: Option<GroupBy>) -> Option<Vec<Row>> {
        let table = self.tables.get(table_name)?;
        let mut result = table.iter().map(|r| r.value().clone()).collect::<Vec<_>>();

        for condition in conditions {
            result = self.filter_rows(table_name, &table, result, condition);
        }

        if let Some(order_by) = order_by {
            let empty = String::new();
            result.sort_by(|a, b| {
                let (a_val, b_val) = (a.data.get(&order_by.field).unwrap_or(&empty), b.data.get(&order_by.field).unwrap_or(&empty));
                if order_by.ascending { a_val.cmp(b_val) } else { b_val.cmp(a_val) }
            });
        }

        if let Some(group_by) = group_by {
            let mut grouped: HashMap<String, Vec<Row>> = HashMap::new();
            for row in result {
                grouped.entry(row.data.get(&group_by.field).cloned().unwrap_or_default()).or_default().push(row);
            }
            result = match group_by.aggregate {
                Aggregate::Count => grouped.into_iter().map(|(key, rows)| Row {
                    id: rows[0].id,
                    data: HashMap::from([(group_by.field.clone(), key), ("count".to_string(), rows.len().to_string())]),
                }).collect(),
                Aggregate::Sum(field) => grouped.into_iter().map(|(key, rows)| Row {
                    id: rows[0].id,
                    data: HashMap::from([(group_by.field.clone(), key), (field.clone(), rows.iter().filter_map(|r| r.data.get(&field).and_then(|v| v.parse::<i32>().ok())).sum::<i32>().to_string())]),
                }).collect(),
            };
        }
        Some(result)
    }

    pub async fn multi_join(&self, joins: Vec<(&str, &str, &str, &str)>) -> Option<Vec<Vec<Row>>> {
        let mut results = Vec::new();
        for (table1, field1, table2, field2) in joins {
            let (t1, t2) = (self.tables.get(table1)?, self.tables.get(table2)?);
            let mut result = Vec::new();
            for row1 in t1.iter() {
                if let Some(value) = row1.data.get(field1) {
                    if let Some(index_ref) = self.indexes.get(table2) {
                        if let Some(index) = index_ref.get(field2) {
                            if let Some(ids) = index.get(value) {
                                for id in ids.iter() {
                                    if let Some(row2) = t2.get(id) {
                                        result.push(vec![row1.value().clone(), row2.value().clone()]);
                                    }
                                }
                            }
                        } else {
                            for row2 in t2.iter() {
                                if row2.data.get(field2).map_or(false, |v| v == value) {
                                    result.push(vec![row1.value().clone(), row2.value().clone()]);
                                }
                            }
                        }
                    } else {
                        for row2 in t2.iter() {
                            if row2.data.get(field2).map_or(false, |v| v == value) {
                                result.push(vec![row1.value().clone(), row2.value().clone()]);
                            }
                        }
                    }
                }
            }
            results.push(result);
        }

        if results.is_empty() { return Some(vec![]); }
        let mut final_result = results[0].clone();
        for i in 1..results.len() {
            let mut new_result = Vec::new();
            for row_set1 in &final_result {
                for row_set2 in &results[i] {
                    let mut combined = row_set1.clone();
                    combined.extend(row_set2.clone());
                    new_result.push(combined);
                }
            }
            final_result = new_result;
        }
        Some(final_result)
    }
}