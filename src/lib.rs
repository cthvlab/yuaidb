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
    unique: Option<bool>,
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

#[derive(Debug, Clone)]
pub enum Condition {
    Eq(String, String),
    Lt(String, String),
    Gt(String, String),
    Contains(String, String),
    In(String, Vec<String>),
    Between(String, String, String),
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
    config: Arc<DbConfig>,
}

#[derive(Clone, Default)]
pub struct SelectQuery {
    table: String,
    fields: Vec<String>,
    alias: String,
    joins: Vec<(String, String, String, String)>,
    where_clauses: Vec<Vec<Condition>>,
}

#[derive(Default)]
pub struct InsertQuery {
    table: String,
    values: HashMap<String, String>,
}

#[derive(Default)]
pub struct UpdateQuery {
    table: String,
    sets: HashMap<String, String>,
    where_clauses: Vec<Vec<Condition>>,
}

#[derive(Default)]
pub struct DeleteQuery {
    table: String,
    where_clauses: Vec<Vec<Condition>>,
}

macro_rules! add_condition {
    ($method:ident, $variant:ident, $self:ident, $field:expr, $value:expr) => {
        pub fn $method<T: Into<String>>(mut $self, field: &str, value: T) -> Self {
            $self.ensure_where_clause();
            $self.where_clauses.last_mut().unwrap().push(Condition::$variant(field.to_string(), value.into()));
            $self
        }
    };
}

macro_rules! impl_filterable {
    ($struct:ident) => {
        impl $struct {
            fn ensure_where_clause(&mut self) {
                if self.where_clauses.is_empty() {
                    self.where_clauses.push(Vec::new());
                }
            }

            add_condition!(where_eq, Eq, self, field, value);
            add_condition!(where_lt, Lt, self, field, value);
            add_condition!(where_gt, Gt, self, field, value);
            add_condition!(where_contains, Contains, self, field, value);

            pub fn where_in<T: Into<String>>(mut self, field: &str, values: Vec<T>) -> Self {
                self.ensure_where_clause();
                self.where_clauses.last_mut().unwrap().push(Condition::In(
                    field.to_string(),
                    values.into_iter().map(Into::into).collect()
                ));
                self
            }

            pub fn where_between<T: Into<String>>(mut self, field: &str, min: T, max: T) -> Self {
                self.ensure_where_clause();
                self.where_clauses.last_mut().unwrap().push(Condition::Between(
                    field.to_string(),
                    min.into(),
                    max.into()
                ));
                self
            }

            pub fn and_where(mut self, condition: Condition) -> Self {
                self.ensure_where_clause();
                self.where_clauses.last_mut().unwrap().push(condition);
                self
            }

            pub fn or_where(mut self, condition: Condition) -> Self {
                self.where_clauses.push(vec![condition]);
                self
            }

            pub fn and_group(mut self, f: impl FnOnce(&mut Self)) -> Self {
                let mut group = Self::default();
                f(&mut group);
                self.ensure_where_clause();
                self.where_clauses.last_mut().unwrap().extend(group.where_clauses.into_iter().flatten());
                self
            }

            pub fn or_group(mut self, f: impl FnOnce(&mut Self)) -> Self {
                let mut group = Self::default();
                f(&mut group);
                self.where_clauses.extend(group.where_clauses);
                self
            }
        }
    };
}

impl_filterable!(SelectQuery);
impl_filterable!(UpdateQuery);
impl_filterable!(DeleteQuery);

impl SelectQuery {
    pub fn new(table: &str) -> Self {
        Self {
            table: table.to_string(),
            fields: vec!["*".to_string()],
            alias: "".to_string(),
            joins: vec![],
            where_clauses: Vec::new(),
        }
    }

    pub fn fields(mut self, fields: Vec<&str>) -> Self {
        self.fields = fields.into_iter().map(|s| s.to_string()).collect();
        self
    }

    pub fn alias(mut self, alias: &str) -> Self {
        self.alias = alias.to_string();
        self
    }

    pub fn join(mut self, table: &str, alias: &str, on_left: &str, on_right: &str) -> Self {
        self.joins.push((table.to_string(), alias.to_string(), on_left.to_string(), on_right.to_string()));
        self
    }

    pub fn where_clause(self, condition: &str) -> Self {
        let parts: Vec<&str> = condition.split(' ').collect();
        if parts.len() == 3 && parts[1] == "=" {
            let field = parts[0];
            let value = parts[2].trim_matches('\'');
            self.where_eq(field, value)
        } else {
            println!("Неподдерживаемый формат условия: {}", condition);
            self
        }
    }

    pub async fn execute(&self, db: &Database) -> Option<Vec<HashMap<String, String>>> {
        db.execute_select(self.clone()).await
    }
}

impl InsertQuery {
    pub fn values(mut self, values: Vec<(&str, &str)>) -> Self {
        self.values = values.into_iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        self
    }

    pub async fn execute(self, db: &Database) {
        db.execute_insert(self).await;
    }
}

impl UpdateQuery {
    pub fn set(mut self, field: &str, value: &str) -> Self {
        self.sets.insert(field.to_string(), value.to_string());
        self
    }

    pub async fn execute(self, db: &Database) {
        db.execute_update(self).await;
    }
}

impl DeleteQuery {
    pub async fn execute(self, db: &Database) {
        db.execute_delete(self).await;
    }
}

impl Database {
    pub async fn new(data_dir: &str, config_file: &str) -> Self {
        let config_str = tokio::fs::read_to_string(config_file).await.unwrap_or_default();
        let config = Arc::new(toml::from_str(&config_str).unwrap_or_default());
        let db = Database {
            tables: Arc::new(DashMap::new()),
            indexes: Arc::new(DashMap::new()),
            fulltext_indexes: Arc::new(DashMap::new()),
            data_dir: data_dir.to_string(),
            config_file: config_file.to_string(),
            join_cache: Arc::new(DashMap::new()),
            config,
        };
        create_dir_all(data_dir).await.unwrap_or(());
        db.load_tables_from_disk().await;
        let db_clone = db.clone();
        tokio::spawn(async move { db_clone.watch_config().await });
        db
    }

    fn get_unique_field(&self, table_name: &str) -> Option<String> {
        self.config.tables
            .iter()
            .find(|t| t.name == table_name)
            .and_then(|table_config| {
                table_config.fields
                    .iter()
                    .find(|f| f.unique.unwrap_or(false))
                    .map(|f| f.name.clone())
            })
    }

    async fn watch_config(&self) {
        let mut last_content = String::new();
        loop {
            if let Ok(content) = tokio::fs::read_to_string(&self.config_file).await {
                if content != last_content {
                    println!("Обновление конфигурации...");
                    let config: DbConfig = toml::from_str(&content).unwrap_or_default();
                    *Arc::get_mut(&mut self.config.clone()).unwrap() = config;
                    self.apply_config().await;
                    last_content = content;
                }
            }
            sleep(Duration::from_secs(5)).await;
        }
    }

    pub fn get_table(&self, table_name: &str) -> Option<dashmap::mapref::one::Ref<String, Arc<DashMap<i32, Row>>>> {
        self.tables.get(table_name)
    }

    pub fn select(&self, table: &str) -> SelectQuery {
        SelectQuery {
            table: table.to_string(),
            alias: table.to_string(),
            ..Default::default()
        }
    }

    pub fn start_insert(&self, table: &str) -> InsertQuery {
        InsertQuery {
            table: table.to_string(),
            ..Default::default()
        }
    }

    pub fn update(&self, table: &str) -> UpdateQuery {
        UpdateQuery {
            table: table.to_string(),
            ..Default::default()
        }
    }

    pub fn delete(&self, table: &str) -> DeleteQuery {
        DeleteQuery {
            table: table.to_string(),
            ..Default::default()
        }
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
        println!("Сохранена таблица {} в файл {}", table_name, path);
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

    pub async fn create_table(&self, table_name: &str) {
        let table = Arc::new(DashMap::new());
        self.tables.insert(table_name.to_string(), table.clone());
    }

    pub async fn insert_row(&self, table_name: &str, row: Row) {
        let table = self.tables.entry(table_name.to_string())
            .or_insert_with(|| Arc::new(DashMap::new()))
            .value()
            .clone();

        table.insert(row.id, row.clone());
        self.update_indexes(table_name, &row, false).await;
        self.save_table(table_name, &table).await;
        self.join_cache.retain(|key, _| !key.contains(table_name));
    }

    fn filter_rows(&self, table_name: &str, rows: Vec<Row>, where_clauses: &[Vec<Condition>]) -> Vec<Row> {
        let mut filtered_rows = Vec::new();
        for or_group in where_clauses {
            let mut group_rows = rows.clone();
            for condition in or_group {
                group_rows = match condition {
                    Condition::Eq(field, value) => {
                        if let Some(index_map) = self.indexes.get(table_name) {
                            if let Some(index) = index_map.get(field) {
                                index.get(value).map_or(vec![], |ids| {
                                    ids.iter().filter_map(|id| self.tables.get(table_name).and_then(|t| t.get(&id).map(|r| r.value().clone()))).collect()
                                })
                            } else {
                                group_rows.into_iter().filter(|row| row.data.get(field).map_or(false, |v| v == value)).collect()
                            }
                        } else {
                            group_rows.into_iter().filter(|row| row.data.get(field).map_or(false, |v| v == value)).collect()
                        }
                    }
                    Condition::Lt(field, value) => group_rows.into_iter().filter(|row| row.data.get(field).map_or(false, |v| v < value)).collect(),
                    Condition::Gt(field, value) => group_rows.into_iter().filter(|row| row.data.get(field).map_or(false, |v| v > value)).collect(),
                    Condition::Contains(field, value) => {
                        if let Some(ft_index_map) = self.fulltext_indexes.get(table_name) {
                            if let Some(ft_index) = ft_index_map.get(field) {
                                let value_lower = value.to_lowercase();
                                let mut ids: Vec<i32> = Vec::new();
                                for entry in ft_index.iter() {
                                    if entry.key().contains(&value_lower) {
                                        ids.extend(entry.value().clone());
                                    }
                                }
                                ids.sort_unstable();
                                ids.dedup();
                                ids.into_iter().filter_map(|id| self.tables.get(table_name).and_then(|t| t.get(&id).map(|r| r.value().clone()))).collect()
                            } else {
                                group_rows.into_iter().filter(|row| row.data.get(field).map_or(false, |v| v.to_lowercase().contains(&value.to_lowercase()))).collect()
                            }
                        } else {
                            group_rows.into_iter().filter(|row| row.data.get(field).map_or(false, |v| v.to_lowercase().contains(&value.to_lowercase()))).collect()
                        }
                    }
                    Condition::In(field, values) => group_rows.into_iter().filter(|row| row.data.get(field).map_or(false, |v| values.contains(v))).collect(),
                    Condition::Between(field, min, max) => group_rows.into_iter().filter(|row| row.data.get(field).map_or(false, |v| v >= min && v <= max)).collect(),
                };
            }
            filtered_rows.extend(group_rows);
        }
        filtered_rows.sort_by(|a, b| a.id.cmp(&b.id));
        filtered_rows.dedup_by(|a, b| a.id == b.id);
        filtered_rows
    }

    pub async fn execute_select(&self, query: SelectQuery) -> Option<Vec<HashMap<String, String>>> {
        let table = self.tables.get(&query.table)?;
        let mut rows: Vec<Vec<Row>> = Vec::new();

        for row in table.iter() {
            let mut row_set = vec![row.value().clone()];
            
            for (join_table, _join_alias, on_left, on_right) in &query.joins {
                if let Some(join_table_data) = self.tables.get(join_table) {
                    let left_field = on_left.split('.').nth(1).unwrap_or(on_left);
                    let right_field = on_right.split('.').nth(1).unwrap_or(on_right);
                    
                    for join_row in join_table_data.iter() {
                        let left_value = join_row.data.get(left_field);
                        let right_value = row_set[0].data.get(right_field);
                        if left_value == right_value {
                            row_set.push(join_row.value().clone());
                            break;
                        }
                    }
                }
            }
            rows.push(row_set);
        }

        let filtered_rows = if !query.where_clauses.is_empty() {
            rows.into_iter().filter(|row_set| {
                let first_row = row_set.first().unwrap();
                query.where_clauses.iter().any(|clause_group| {
                    clause_group.iter().all(|condition| match condition {
                        Condition::Eq(field, value) => {
                            first_row.data.get(field).map_or(false, |v| v == value)
                        }
                        Condition::Lt(field, value) => {
                            first_row.data.get(field).map_or(false, |v| v < value)
                        }
                        Condition::Gt(field, value) => {
                            first_row.data.get(field).map_or(false, |v| v > value)
                        }
                        Condition::Contains(field, value) => {
                            first_row.data.get(field).map_or(false, |v| v.to_lowercase().contains(&value.to_lowercase()))
                        }
                        Condition::In(field, values) => {
                            first_row.data.get(field).map_or(false, |v| values.contains(v))
                        }
                        Condition::Between(field, min, max) => {
                            first_row.data.get(field).map_or(false, |v| v >= min && v <= max)
                        }
                    })
                })
            }).collect()
        } else {
            rows
        };

        let mut results = Vec::new();
        for row_set in filtered_rows {
            let mut result_row = HashMap::new();
            for (i, row) in row_set.iter().enumerate() {
                let alias = if i == 0 { &query.alias } else { &query.joins[i - 1].1 };
                for field in &query.fields {
                    if field == "*" {
                        for (k, v) in &row.data {
                            result_row.insert(format!("{}.{}", alias, k), v.clone());
                        }
                    } else if field.contains('.') {
                        let (field_alias, field_name) = field.split_once('.').unwrap();
                        if field_alias == alias {
                            if let Some(value) = row.data.get(field_name) {
                                result_row.insert(field.clone(), value.clone());
                            }
                        }
                    } else if query.joins.is_empty() {
                        if let Some(value) = row.data.get(field) {
                            result_row.insert(field.clone(), value.clone());
                        }
                    }
                }
            }
            results.push(result_row);
        }

        if results.is_empty() { None } else { Some(results) }
    }

    async fn execute_insert(&self, query: InsertQuery) {
        let table_name = &query.table;
        let mut values = query.values;

        let unique_field = self.get_unique_field(table_name);

        if let Some(unique_field) = &unique_field {
            if let Some(value) = values.get(unique_field) {
                if let Some(table) = self.tables.get(table_name) {
                    if let Some(existing_row) = table.iter().find(|r| r.data.get(unique_field) == Some(value)) {
                        let mut updated_row = existing_row.value().clone();
                        updated_row.data.extend(values.clone());
                        table.insert(updated_row.id, updated_row.clone());
                        self.update_indexes(table_name, &updated_row, false).await;
                        self.save_table(table_name, &table).await;
                        println!("Обновлена существующая запись с {} = '{}' в таблице {}", unique_field, value, table_name);
                        return;
                    }
                }
            } else {
                println!("Уникальное поле {} отсутствует в данных для таблицы {}, вставка пропущена", unique_field, table_name);
                return;
            }
        }

        let id = if let Some(id_str) = values.remove("id") {
            id_str.parse::<i32>().unwrap_or_else(|_| {
                if let Some(table) = self.tables.get(table_name) {
                    table.iter().map(|r| r.id).max().unwrap_or(0) + 1
                } else {
                    1
                }
            })
        } else {
            if let Some(table) = self.tables.get(table_name) {
                table.iter().map(|r| r.id).max().unwrap_or(0) + 1
            } else {
                self.create_table(table_name).await;
                1
            }
        };

        if unique_field.is_none() {
            if let Some(table) = self.tables.get(table_name) {
                if table.contains_key(&id) {
                    println!("Запись с id {} уже существует в таблице {}, пропускаем вставку", id, table_name);
                    return;
                }
            }
        }

        let row = Row { id, data: values };
        self.insert_row(table_name, row).await;
    }

    async fn execute_update(&self, query: UpdateQuery) {
        if let Some(table) = self.tables.get(&query.table) {
            let mut to_update = table.iter().map(|r| r.value().clone()).collect::<Vec<_>>();
            if !query.where_clauses.is_empty() {
                to_update = self.filter_rows(&query.table, to_update, &query.where_clauses);
            }
            for mut row in to_update {
                self.update_indexes(&query.table, &row, true).await;
                row.data.extend(query.sets.clone());
                self.update_indexes(&query.table, &row, false).await;
                table.insert(row.id, row);
            }
            self.save_table(&query.table, &table).await;
            self.join_cache.retain(|key, _| !key.contains(&query.table));
        }
    }

    async fn execute_delete(&self, query: DeleteQuery) {
        if let Some(table) = self.tables.get(&query.table) {
            let mut to_delete = table.iter().map(|r| r.value().clone()).collect::<Vec<_>>();
            if !query.where_clauses.is_empty() {
                to_delete = self.filter_rows(&query.table, to_delete, &query.where_clauses);
            }
            for row in to_delete {
                self.update_indexes(&query.table, &row, true).await;
                table.remove(&row.id);
            }
            self.save_table(&query.table, &table).await;
            self.join_cache.retain(|key, _| !key.contains(&query.table));
        }
    }
}
