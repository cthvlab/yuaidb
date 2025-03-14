use std::collections::HashMap; // Ключи и значения для быстрого поиска добычи
use std::sync::Arc; // Для безопасного дележа данных между потоками
use std::hash::BuildHasherDefault; // Шаблон для создания хитрых хэш-функций — замок на сундуке
use ahash::AHasher; // Это быстрый и надёжный крипто-генератор который хэширует ключи
use dashmap::DashMap; // Карта для поиска добычи: быстрая, многопоточная, без багов
use serde::{Serialize, Deserialize}; // Магия сериализации — превращаем данные в байты и обратно
use toml; // Парсер TOML — читаем конфиги
use tokio::fs::{File, create_dir_all}; // Файловая система в асинхронном стиле — копаем ямы и прячем сокровища
use tokio::io::{AsyncReadExt, AsyncWriteExt}; // Читаем и пишем байты
use tokio::sync::RwLock; // Замок для данных — один пишет, другие ждут, как в очереди
use tokio::time::{sleep, Duration}; // Таймеры для асинхронных трюков — ждём нужный момент

type Hasher = BuildHasherDefault<AHasher>; // Хэшер — ускоряет поиск!

// Конфиг базы — настройки для всей системы, без путаницы!
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DbConfig {
    tables: Vec<TableConfig>, // Таблицы в одном месте
}

// Пустой конфиг — если всё сломалось, начнём заново!
impl Default for DbConfig {
    fn default() -> Self {
        Self { tables: Vec::new() } // Ноль таблиц — чистый старт!
    }
}

// Описание таблицы — что храним и как зовём!
#[derive(Debug, Serialize, Deserialize, Clone)]
struct TableConfig {
    name: String,              // Имя таблицы, коротко и ясно
    fields: Vec<FieldConfig>, // Поля таблицы
}

// Поля — настройки для данных, без сюрпризов!
#[derive(Debug, Serialize, Deserialize, Clone)]
struct FieldConfig {
    name: String,           // Имя поля 
    indexed: Option<bool>,  // Индекс — для шустрого поиска
    fulltext: Option<bool>, // Полнотекст — ищем по словам
    unique: Option<bool>,   // Уникальность 
    autoincrement: Option<bool>, // Авто-ID — для новых записей!
}

// Строка — данные с ID, просто и надёжно!
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Row {
    pub id: i32,                    // ID — номер в списке дел
    pub data: HashMap<String, String>, // Данные — всё по полочкам!
}

// Условия — фильтры для запросов, без лишнего шума!
#[derive(Debug, Clone)]
enum Condition {
    Eq(String, String),         // Равно — точный удар!
    Lt(String, String),         // Меньше — отсекаем лишнее!
    Gt(String, String),         // Больше — только крупные куски!
    Contains(String, String),   // Содержит — ищем иголку!
    In(String, Vec<String>),    // В списке — проверка по шпаргалке!
    Between(String, String, String), // Между — диапазон на миллиметровку!
}

// База — центр управления, никаких сбоев!
#[derive(Clone)]
pub struct Database {
    tables: Arc<DashMap<String, Arc<DashMap<i32, Row, Hasher>>, Hasher>>, // Таблицы — хранилище с турбо-доступом!
    indexes: Arc<DashMap<String, Arc<DashMap<String, Arc<DashMap<String, Vec<i32>, Hasher>>, Hasher>>, Hasher>>, // Индексы — шпаргалка для скорости!
    fulltext_indexes: Arc<DashMap<String, Arc<DashMap<String, Arc<DashMap<String, Vec<i32>, Hasher>>, Hasher>>, Hasher>>, // Полнотекст 
    data_dir: String,           // Папка данных — наш жёсткий диск!
    config_file: String,        // Файл конфига — инструкция к запуску!
    join_cache: Arc<DashMap<String, Vec<(Row, Row)>, Hasher>>, // Кэш связок — ускорение на миллион!
    config: Arc<RwLock<DbConfig>>, // Конфиг с замком — безопасность на уровне!
}

// Запрос — план действий, без долгих раздумий!
#[derive(Debug, Default, Clone)]
pub struct Query {
    table: String,                    // Таблица — куда лезем
    fields: Vec<String>,             // Поля — что берём
    alias: String,                   // Псевдоним — для связей!
    joins: Vec<(String, String, String, String)>, // Связи — собираем пазл!
    where_clauses: Vec<Vec<Condition>>, // Условия — отсекаем лишнее!
    values: Vec<HashMap<String, String>>, // Данные — свежий улов!
    op: QueryOp,                     // Операция — что творим?
}

// Тип операции — команда для базы, коротко и чётко!
#[derive(Debug, Clone, Default)]
enum QueryOp {
    #[default]
    Select,  // Читаем
    Insert,  // Добавляем
    Update,  // Обновляем
    Delete,  // Удаляем
}

// Макрос для сборки запросов — автоматика!
macro_rules! query_builder {
    ($method:ident, $op:ident) => {
        // Метод-запускатор: берём таблицу и готовим запрос!
        pub fn $method(&self, table: &str) -> Query {
            Query {
                table: table.to_string(),       // Имя таблицы 
                alias: table.to_string(),       // Псевдоним 
                op: QueryOp::$op,              // Операция 
                fields: vec!["*".to_string()], // Берём всё — жадность юного инженера :)
                ..Default::default()           // Остальное по умолчанию — меньше кода!
            }
        }
    };
}

// Макрос для условий — добавляем фильтры без лишней возни!
macro_rules! add_condition {
    ($method:ident, $variant:ident) => {
        // Метод-фильтратор: кидаем поле и значение в запрос
        pub fn $method<T: Into<String>>(mut self, field: &str, value: T) -> Self {
            // Если фильтров нет, создаём пустой список
            if self.where_clauses.is_empty() { self.where_clauses.push(Vec::new()); }
            // Добавляем условие — точность наше всё!
            self.where_clauses.last_mut().unwrap().push(Condition::$variant(field.to_string(), value.into()));
            self // Возвращаем себя — цепочки!
        }
    };
}

// позволяет писать код в стиле цепочек - SQL Like синтаксис
impl Query {
    // Задаём поля — что хватать из базы
    pub fn fields(mut self, fields: Vec<&str>) -> Self {
        self.fields = fields.into_iter().map(|s| s.to_string()).collect();
        self // Цепочка — наше всё!
    }
    // Псевдоним 
    pub fn alias(mut self, alias: &str) -> Self {
        self.alias = alias.to_string();
        self // Ещё одна цепочка, ура!
    }
    // Джоин — связываем таблицы, как конструктор!
    pub fn join(mut self, table: &str, alias: &str, on_left: &str, on_right: &str) -> Self {
        self.joins.push((table.to_string(), alias.to_string(), on_left.to_string(), on_right.to_string()));
        self // Цепляем дальше!
    }
    // Значения — кидаем данные в запрос, без лишних рук!
    pub fn values(mut self, values: Vec<Vec<(&str, &str)>>) -> Self {
        self.values = values.into_iter()
            .map(|row| row.into_iter().map(|(k, v)| (k.to_string(), v.to_string())).collect())
            .collect();
        self // Цепочка не рвётся!
    }

    // Условия — фильтры для точных ударов!
    add_condition!(where_eq, Eq);     // Равно — бьём в яблочко!
    add_condition!(where_lt, Lt);     // Меньше — отсекаем гигантов!
    add_condition!(where_gt, Gt);     // Больше — мелочь не берём!
    add_condition!(where_contains, Contains); // Содержит — ищем тайники!

    // Где "в списке" — проверка по шпаргалке!
    pub fn where_in<T: Into<String>>(mut self, field: &str, values: Vec<T>) -> Self {
        if self.where_clauses.is_empty() { self.where_clauses.push(Vec::new()); }
        self.where_clauses.last_mut().unwrap().push(Condition::In(field.to_string(), values.into_iter().map(Into::into).collect()));
        self // Цепочка живёт!
    }

    // Где "между" — диапазон для умников!
    pub fn where_between<T: Into<String>>(mut self, field: &str, min: T, max: T) -> Self {
        if self.where_clauses.is_empty() { self.where_clauses.push(Vec::new()); }
        self.where_clauses.last_mut().unwrap().push(Condition::Between(field.to_string(), min.into(), max.into()));
        self // Цепочка — наш герой!
    }

    // Выполняем запрос — время жать на кнопку!
    pub async fn execute(self, db: &Database) -> Option<Vec<HashMap<String, String>>> {
        match self.op {
            QueryOp::Select => db.execute_select(self).await, // Читаем
            QueryOp::Insert => { db.execute_insert(self).await; None } // Вставляем
            QueryOp::Update => { db.execute_update(self).await; None } // Обновляем
            QueryOp::Delete => { db.execute_delete(self).await; None } // Удаляем
        }
    }
}
// "пульт управления" для базы данных
impl Database {
    pub async fn new(data_dir: &str, config_file: &str) -> Self {
        let config_str = tokio::fs::read_to_string(config_file).await.unwrap_or_default();
        let config = Arc::new(RwLock::new(toml::from_str(&config_str).unwrap_or_default()));
        let db = Self {
            tables: Arc::new(DashMap::with_hasher(BuildHasherDefault::<AHasher>::default())),
            indexes: Arc::new(DashMap::with_hasher(BuildHasherDefault::<AHasher>::default())),
            fulltext_indexes: Arc::new(DashMap::with_hasher(BuildHasherDefault::<AHasher>::default())),
            data_dir: data_dir.to_string(),
            config_file: config_file.to_string(),
            join_cache: Arc::new(DashMap::with_hasher(BuildHasherDefault::<AHasher>::default())),
            config,
        };
        create_dir_all(data_dir).await.unwrap_or(());
        db.load_tables_from_disk().await;
        let db_clone = db.clone();
        tokio::spawn(async move { db_clone.watch_config().await });
        db
    }

    query_builder!(select, Select);
    query_builder!(insert, Insert);
    query_builder!(update, Update);
    query_builder!(delete, Delete);

    async fn get_unique_field(&self, table_name: &str) -> Option<String> {
        self.config.read().await.tables.iter()
            .find(|t| t.name == table_name)
            .and_then(|t| t.fields.iter().find(|f| f.unique.unwrap_or(false)).map(|f| f.name.clone()))
    }

    async fn get_autoincrement_field(&self, table_name: &str) -> Option<String> {
        self.config.read().await.tables.iter()
            .find(|t| t.name == table_name)
            .and_then(|t| t.fields.iter().find(|f| f.autoincrement.unwrap_or(false)).map(|f| f.name.clone()))
    }

    async fn watch_config(&self) {
        let mut last_content = String::new();
        loop {
            if let Ok(content) = tokio::fs::read_to_string(&self.config_file).await {
                if content != last_content {
                    *self.config.write().await = toml::from_str(&content).unwrap_or_default();
                    self.apply_config().await;
                    last_content = content;
                }
            }
            sleep(Duration::from_secs(5)).await;
        }
    }

    async fn load_tables_from_disk(&self) {
        if let Ok(mut entries) = tokio::fs::read_dir(&self.data_dir).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                if entry.path().extension() == Some("bin".as_ref()) {
                    let table_name = entry.path().file_stem().unwrap().to_str().unwrap().to_string();
                    let mut file = File::open(&entry.path()).await.unwrap();
                    let mut buffer = Vec::new();
                    file.read_to_end(&mut buffer).await.unwrap();
                    if let Ok(rows) = bincode::deserialize::<HashMap<i32, Row>>(&buffer) {
                        let table = Arc::new(DashMap::with_hasher(BuildHasherDefault::<AHasher>::default()));
                        let unique_field = self.get_unique_field(&table_name).await;
                        let mut seen = std::collections::HashSet::new();
                        for (id, row) in rows {
                            if let Some(ref field) = unique_field {
                                if let Some(value) = row.data.get(field) {
                                    if seen.contains(value) { continue; }
                                    seen.insert(value.clone());
                                }
                            }
                            table.insert(id, row);
                        }
                        self.tables.insert(table_name.clone(), table);
                        self.rebuild_indexes(&table_name).await;
                    }
                }
            }
        }
    }

    async fn save_table(&self, table_name: &str) {
        if let Some(table) = self.tables.get(table_name) {
            let path = format!("{}/{}.bin", self.data_dir, table_name);
            let rows: HashMap<i32, Row> = table.iter().map(|r| (*r.key(), r.value().clone())).collect();
            let encoded = bincode::serialize(&rows).unwrap();
            File::create(&path).await.unwrap().write_all(&encoded).await.unwrap();
        }
    }

    async fn rebuild_indexes(&self, table_name: &str) {
        if let Some(table) = self.tables.get(table_name) {
            let config = self.config.read().await;
            if let Some(table_config) = config.tables.iter().find(|t| t.name == table_name) {
                for field in &table_config.fields {
                    let (indexed, fulltext) = (field.indexed.unwrap_or(false), field.fulltext.unwrap_or(false));
                    if indexed || fulltext {
                        let index = DashMap::with_hasher(BuildHasherDefault::<AHasher>::default());
                        for row in table.iter() {
                            if let Some(value) = row.data.get(&field.name) {
                                if indexed {
                                    index.entry(value.clone()).or_insert_with(Vec::new).push(row.id);
                                    self.indexes.entry(table_name.to_string())
                                        .or_insert_with(|| Arc::new(DashMap::with_hasher(BuildHasherDefault::<AHasher>::default())))
                                        .insert(field.name.clone(), Arc::new(index.clone()));
                                }
                                if fulltext {
                                    for word in value.split_whitespace() {
                                        index.entry(word.to_lowercase()).or_insert_with(Vec::new).push(row.id);
                                    }
                                    self.fulltext_indexes.entry(table_name.to_string())
                                        .or_insert_with(|| Arc::new(DashMap::with_hasher(BuildHasherDefault::<AHasher>::default())))
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
        let config = self.config.read().await;
        for table_config in &config.tables {
            if !self.tables.contains_key(&table_config.name) {
                self.tables.insert(table_config.name.clone(), Arc::new(DashMap::with_hasher(BuildHasherDefault::<AHasher>::default())));
                self.save_table(&table_config.name).await;
            }
            self.rebuild_indexes(&table_config.name).await;
        }
    }

    async fn update_indexes(&self, table_name: &str, row: &Row, remove: bool) {
        let table_name = table_name.to_string();
        for (field, value) in &row.data {
            if let Some(index_map) = self.indexes.get(&table_name) {
                if let Some(index) = index_map.get(field) {
                    if remove {
                        if let Some(mut ids) = index.get_mut(value) { ids.retain(|&id| id != row.id); }
                    } else {
                        index.entry(value.clone()).or_insert_with(Vec::new).push(row.id);
                    }
                }
            }
            if let Some(ft_index_map) = self.fulltext_indexes.get(&table_name) {
                if let Some(ft_index) = ft_index_map.get(field) {
                    for word in value.split_whitespace() {
                        let word = word.to_lowercase();
                        if remove {
                            if let Some(mut ids) = ft_index.get_mut(&word) { ids.retain(|&id| id != row.id); }
                        } else {
                            ft_index.entry(word).or_insert_with(Vec::new).push(row.id);
                        }
                    }
                }
            }
        }
    }

    fn filter_rows(&self, table_name: &str, rows: Vec<Row>, where_clauses: &[Vec<Condition>]) -> Vec<Row> {
        let mut filtered = Vec::new();
        for or_group in where_clauses {
            let mut group_rows = rows.clone();
            for condition in or_group {
                group_rows = match condition {
                    Condition::Eq(field, value) => self.index_filter(table_name, field, value, group_rows, |v, val| v == val),
                    Condition::Lt(field, value) => self.index_filter(table_name, field, value, group_rows, |v, val| v < val),
                    Condition::Gt(field, value) => self.index_filter(table_name, field, value, group_rows, |v, val| v > val),
                    Condition::Contains(field, value) => self.fulltext_filter(table_name, field, value, group_rows),
                    Condition::In(field, values) => group_rows.into_iter().filter(|r| r.data.get(field).map_or(false, |v| values.contains(v))).collect(),
                    Condition::Between(field, min, max) => group_rows.into_iter().filter(|r| r.data.get(field).map_or(false, |v| v >= min && v <= max)).collect(),
                };
            }
            filtered.extend(group_rows);
        }
        filtered.sort_by_key(|r| r.id);
        filtered.dedup_by_key(|r| r.id);
        filtered
    }

    fn index_filter<F>(&self, table_name: &str, field: &str, value: &str, rows: Vec<Row>, pred: F) -> Vec<Row>
    where F: Fn(&str, &str) -> bool {
        if let Some(index_map) = self.indexes.get(table_name) {
            if let Some(index) = index_map.get(field) {
                return index.get(value).map_or(Vec::new(), |ids| {
                    ids.iter().filter_map(|id| self.tables.get(table_name).and_then(|t| t.get(id).map(|r| r.clone()))).collect()
                });
            }
        }
        rows.into_iter().filter(|r| r.data.get(field).map_or(false, |v| pred(v, value))).collect()
    }

    fn fulltext_filter(&self, table_name: &str, field: &str, value: &str, rows: Vec<Row>) -> Vec<Row> {
        let value_lower = value.to_lowercase();
        if let Some(ft_index_map) = self.fulltext_indexes.get(table_name) {
            if let Some(ft_index) = ft_index_map.get(field) {
                let mut ids = Vec::new();
                for entry in ft_index.iter() {
                    if entry.key().contains(&value_lower) { ids.extend(entry.value().clone()); }
                }
                ids.sort_unstable();
                ids.dedup();
                return ids.into_iter().filter_map(|id| self.tables.get(table_name).and_then(|t| t.get(&id).map(|r| r.clone()))).collect();
            }
        }
        rows.into_iter().filter(|r| r.data.get(field).map_or(false, |v| v.to_lowercase().contains(&value_lower))).collect()
    }

    async fn execute_select(&self, query: Query) -> Option<Vec<HashMap<String, String>>> {
        let table = self.tables.get(&query.table)?;
        let rows: Vec<(String, Row)> = table.iter().map(|r| (query.alias.clone(), r.clone())).collect();
        let mut joined_rows: Vec<Vec<(String, Row)>> = rows.into_iter().map(|r| vec![r]).collect();

        for (join_table, join_alias, on_left, on_right) in &query.joins {
            if let Some(join_table_data) = self.tables.get(join_table) {
                let left_field = on_left.split('.').nth(1).unwrap_or(on_left);
                let right_field = on_right.split('.').nth(1).unwrap_or(on_right);
                joined_rows = joined_rows.into_iter().filter_map(|mut row_set| {
                    let right_value = row_set[0].1.data.get(right_field);
                    join_table_data.iter()
                        .find(|jr| jr.data.get(left_field) == right_value)
                        .map(|jr| {
                            row_set.push((join_alias.clone(), jr.clone()));
                            row_set
                        })
                }).collect();
            }
        }

        let filtered_rows = if !query.where_clauses.is_empty() {
            joined_rows.into_iter().filter(|row_set| {
                query.where_clauses.iter().any(|or_group| {
                    or_group.iter().all(|condition| {
                        match condition {
                            Condition::Eq(field, value) => {
                                let (alias, field_name) = field.split_once('.').unwrap_or(("", field));
                                let row = row_set.iter().find(|(a, _)| a == alias || (alias.is_empty() && a == &query.alias));
                                row.map_or(false, |(_, r)| r.data.get(field_name).map_or(false, |v| v == value))
                            }
                            Condition::Lt(field, value) => {
                                let (alias, field_name) = field.split_once('.').unwrap_or(("", field));
                                let row = row_set.iter().find(|(a, _)| a == alias || (alias.is_empty() && a == &query.alias));
                                row.map_or(false, |(_, r)| r.data.get(field_name).map_or(false, |v| v < value))
                            }
                            Condition::Gt(field, value) => {
                                let (alias, field_name) = field.split_once('.').unwrap_or(("", field));
                                let row = row_set.iter().find(|(a, _)| a == alias || (alias.is_empty() && a == &query.alias));
                                row.map_or(false, |(_, r)| r.data.get(field_name).map_or(false, |v| v > value))
                            }
                            Condition::Contains(field, value) => {
                                let (alias, field_name) = field.split_once('.').unwrap_or(("", field));
                                let row = row_set.iter().find(|(a, _)| a == alias || (alias.is_empty() && a == &query.alias));
                                row.map_or(false, |(_, r)| r.data.get(field_name).map_or(false, |v| v.to_lowercase().contains(&value.to_lowercase())))
                            }
                            Condition::In(field, values) => {
                                let (alias, field_name) = field.split_once('.').unwrap_or(("", field));
                                let row = row_set.iter().find(|(a, _)| a == alias || (alias.is_empty() && a == &query.alias));
                                row.map_or(false, |(_, r)| r.data.get(field_name).map_or(false, |v| values.contains(v)))
                            }
                            Condition::Between(field, min, max) => {
                                let (alias, field_name) = field.split_once('.').unwrap_or(("", field));
                                let row = row_set.iter().find(|(a, _)| a == alias || (alias.is_empty() && a == &query.alias));
                                row.map_or(false, |(_, r)| r.data.get(field_name).map_or(false, |v| v >= min && v <= max))
                            }
                        }
                    })
                })
            }).collect()
        } else {
            joined_rows
        };

        let results: Vec<_> = filtered_rows.into_iter().map(|row_set| {
            let mut result = HashMap::new();
            for (alias, row) in row_set {
                for field in &query.fields {
                    if field == "*" {
                        for (k, v) in &row.data { result.insert(format!("{}.{}", alias, k), v.clone()); }
                    } else if field.contains('.') {
                        let (field_alias, field_name) = field.split_once('.').unwrap();
                        if field_alias == alias { row.data.get(field_name).map(|v| result.insert(field.clone(), v.clone())); }
                    } else if query.joins.is_empty() {
                        row.data.get(field).map(|v| result.insert(field.clone(), v.clone()));
                    }
                }
            }
            result
        }).collect();

        if results.is_empty() { None } else { Some(results) }
    }

    async fn execute_insert(&self, query: Query) {
        let autoincrement_field = self.get_autoincrement_field(&query.table).await;
        let table_data = self.tables.entry(query.table.clone())
            .or_insert_with(|| Arc::new(DashMap::with_hasher(BuildHasherDefault::<AHasher>::default())))
            .clone();

        for mut query_values in query.values {
            let mut id = if let Some(field) = &autoincrement_field {
                if let Some(value) = query_values.get(field) {
                    value.parse::<i32>().unwrap_or_else(|_| {
                        table_data.iter().map(|r| r.id).max().unwrap_or(0) + 1
                    })
                } else {
                    table_data.iter().map(|r| r.id).max().unwrap_or(0) + 1
                }
            } else {
                table_data.iter().map(|r| r.id).max().unwrap_or(0) + 1
            };

            while table_data.contains_key(&id) {
                id += 1;
            }

            if let Some(field) = &autoincrement_field {
                query_values.insert(field.clone(), id.to_string());
            }

            let row = Row { id, data: query_values };
            table_data.insert(row.id, row.clone());
            self.update_indexes(&query.table, &row, false).await;
        }
        self.save_table(&query.table).await;
        self.join_cache.retain(|key, _| !key.contains(&query.table));
    }

    async fn execute_update(&self, query: Query) {
        if let Some(table) = self.tables.get(&query.table) {
            let mut to_update = table.iter().map(|r| r.clone()).collect::<Vec<_>>();
            if !query.where_clauses.is_empty() {
                to_update = self.filter_rows(&query.table, to_update, &query.where_clauses);
            }

            if let Some(update_values) = query.values.first() {
                for mut row in to_update {
                    self.update_indexes(&query.table, &row, true).await;
                    row.data.extend(update_values.clone());
                    self.update_indexes(&query.table, &row, false).await;
                    table.insert(row.id, row);
                }
                self.save_table(&query.table).await;
                self.join_cache.retain(|key, _| !key.contains(&query.table));
            }
        }
    }

    async fn execute_delete(&self, query: Query) {
        if let Some(table) = self.tables.get(&query.table) {
            let to_delete = self.filter_rows(&query.table, table.iter().map(|r| r.clone()).collect(), &query.where_clauses);
            for row in to_delete {
                self.update_indexes(&query.table, &row, true).await;
                table.remove(&row.id);
            }
            self.save_table(&query.table).await;
            self.join_cache.retain(|key, _| !key.contains(&query.table));
        }
    }
}
