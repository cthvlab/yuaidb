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

// Чтобы можно было принимать пакетно или по одной записи
pub trait IntoValues {
    fn into_values(self) -> Vec<HashMap<String, String>>;
}
impl IntoValues for Vec<(&str, &str)> {
    fn into_values(self) -> Vec<HashMap<String, String>> {
        vec![self.into_iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()]
    }
}
impl IntoValues for Vec<Vec<(&str, &str)>> {
    fn into_values(self) -> Vec<HashMap<String, String>> {
        self.into_iter()
            .map(|row| row.into_iter().map(|(k, v)| (k.to_string(), v.to_string())).collect())
            .collect()
    }
}
    
// Запрос в базу позволяет писать код в стиле цепочек SQL Like
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
    pub fn values<V>(mut self, values: V) -> Self where V: IntoValues {
		self.values = values.into_values();
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
// "Пульт управления" 
impl Database {
    // Создаём базу — как собрать робота с нуля!
    pub async fn new(data_dir: &str, config_file: &str) -> Self {
        // Читаем конфиг — вдруг там секрет успеха!
        let config_str = tokio::fs::read_to_string(config_file).await.unwrap_or_default();
        // Парсим настройки и прячем под замок!
        let config = Arc::new(RwLock::new(toml::from_str(&config_str).unwrap_or_default()));
        // Собираем базу — все полки на месте!
        let db = Self {
            tables: Arc::new(DashMap::with_hasher(BuildHasherDefault::<AHasher>::default())), // Таблицы — наш склад!
            indexes: Arc::new(DashMap::with_hasher(BuildHasherDefault::<AHasher>::default())), // Индексы — шустрые ярлыки!
            fulltext_indexes: Arc::new(DashMap::with_hasher(BuildHasherDefault::<AHasher>::default())), // Полнотекст — словесный радар!
            data_dir: data_dir.to_string(),       // Папка — наш жёсткий диск!
            config_file: config_file.to_string(), // Файл конфига — инструкция!
            join_cache: Arc::new(DashMap::with_hasher(BuildHasherDefault::<AHasher>::default())), // Кэш — ускорение в кармане!
            config,                              // Конфиг с замком — надёжно!
        };
        // Создаём папку для бэкапов данных — без неё никуда!
        create_dir_all(data_dir).await.unwrap_or(());
        // Загружаем таблицы — оживляем базу!
        db.load_tables_from_disk().await;
        // Клонируем и запускаем слежку за конфигом — шпион в деле!
        let db_clone = db.clone();
        tokio::spawn(async move { db_clone.watch_config().await });
        db // Готово — база живая!
    }

    // Запускаторы запросов 
    query_builder!(select, Select); // Читаем
    query_builder!(insert, Insert); // Вставляем
    query_builder!(update, Update); // Обновляем
    query_builder!(delete, Delete); // Удаляем

    // Ищем уникальное поле
    async fn get_unique_field(&self, table_name: &str) -> Option<String> {
        self.config.read().await.tables.iter()
            .find(|t| t.name == table_name) // Находим таблицу
            .and_then(|t| t.fields.iter().find(|f| f.unique.unwrap_or(false)).map(|f| f.name.clone())) // Выцепляем уникальное поле
    }

    // Ищем автоинкремент — кто сам считает?
    async fn get_autoincrement_field(&self, table_name: &str) -> Option<String> {
        self.config.read().await.tables.iter()
            .find(|t| t.name == table_name) // Ищем таблицу — где автоматика?
            .and_then(|t| t.fields.iter().find(|f| f.autoincrement.unwrap_or(false)).map(|f| f.name.clone())) // Хватаем поле с авто-ID
    }

    // Шпион следит за конфигом — глаз не спускает!
    async fn watch_config(&self) {
        let mut last_content = String::new(); // Старый конфиг — чистый ноль!
        loop {
            if let Ok(content) = tokio::fs::read_to_string(&self.config_file).await { // Читаем файл — что нового?
                if content != last_content { // Изменилось? Пора действовать!
                    *self.config.write().await = toml::from_str(&content).unwrap_or_default(); // Обновляем — свежий план!
                    self.apply_config().await; // Применяем — база в тонусе!
                    last_content = content; // Запоминаем — теперь это база!
                }
            }
            sleep(Duration::from_secs(5)).await; // Ждём 5 сек — отдых для шпиона, дайте ему больше отдыха!
        }
    }

    // Грузим таблицы с диска — оживаем базу!
    async fn load_tables_from_disk(&self) {
        // Читаем папку — где наш склад?
        if let Ok(mut entries) = tokio::fs::read_dir(&self.data_dir).await {
            // Проходим по файлам — что тут у нас?
            while let Ok(Some(entry)) = entries.next_entry().await {
                // Берём только .bin — остальное не трогаем!
                if entry.path().extension() == Some("bin".as_ref()) {
                    // Имя таблицы — выдираем из файла!
                    let table_name = entry.path().file_stem().unwrap().to_str().unwrap().to_string();
                    // Открываем файл — лезем в закрома!
                    let mut file = File::open(&entry.path()).await.unwrap();
                    let mut buffer = Vec::new(); // Буфер — наш временный ящик!
                    file.read_to_end(&mut buffer).await.unwrap(); // Читаем всё — до последнего байта!
                    // Распаковываем строки — добыча в руках!
                    if let Ok(rows) = bincode::deserialize::<HashMap<i32, Row>>(&buffer) {
                        // Новая таблица — свежий контейнер!
                        let table = Arc::new(DashMap::with_hasher(BuildHasherDefault::<AHasher>::default()));
                        // Ищем уникальное поле — кто тут особый?
                        let unique_field = self.get_unique_field(&table_name).await;
                        let mut seen = std::collections::HashSet::new(); // Список виденного — дубли в бан!
                        // Проходим по строкам — грузим добро!
                        for (id, row) in rows {
                            if let Some(ref field) = unique_field {
                                if let Some(value) = row.data.get(field) {
                                    if seen.contains(value) { continue; } // Повтор? Пропускаем!
                                    seen.insert(value.clone()); // Новое — записываем!
                                }
                            }
                            table.insert(id, row); // Кидаем в таблицу — порядок!
                        }
                        // Сохраняем таблицу — место занято!
                        self.tables.insert(table_name.clone(), table);
                        // Перестраиваем индексы — ускоряем поиск!
                        self.rebuild_indexes(&table_name).await;
                    }
                }
            }
        }
    }

    // Сохраняем таблицу
    async fn save_table(&self, table_name: &str) {
        // Берём таблицу — есть ли что спасать?
        if let Some(table) = self.tables.get(table_name) {            
            let path = format!("{}/{}.bin", self.data_dir, table_name); // Путь для файла — наш цифровой сейф!
            let rows: HashMap<i32, Row> = table.iter().map(|r| (*r.key(), r.value().clone())).collect();  // Собираем строки — всё в кучу!
            let encoded = bincode::serialize(&rows).unwrap(); // Кодируем — превращаем в байты            
            File::create(&path).await.unwrap().write_all(&encoded).await.unwrap(); // Пишем на диск — теперь не пропадёт!
        }
    }

    // Перестраиваем индексы — ускоряем базу до турбо-режима!
    async fn rebuild_indexes(&self, table_name: &str) {
        // Проверяем таблицу — есть ли что индексировать?
        if let Some(table) = self.tables.get(table_name) {           
            let config = self.config.read().await;  // Читаем конфиг — где наши настройки
            // Ищем таблицу в конфиге — кто тут главный?
            if let Some(table_config) = config.tables.iter().find(|t| t.name == table_name) {
                // Проходим по полям — какие ускоряем?
                for field in &table_config.fields {
                    let (indexed, fulltext) = (field.indexed.unwrap_or(false), field.fulltext.unwrap_or(false));
                    // Если поле индексируемое — вперёд!
                    if indexed || fulltext {
                        // Новый индекс — чистый лист для скорости!
                        let index = DashMap::with_hasher(BuildHasherDefault::<AHasher>::default());
                        // Проходим по строкам — собираем ярлыки!
                        for row in table.iter() {
                            if let Some(value) = row.data.get(&field.name) {
                                if indexed {
                                    // Добавляем в индекс
                                    index.entry(value.clone()).or_insert_with(Vec::new).push(row.id);
                                    // Кидаем в общий список — порядок в доме!
                                    self.indexes.entry(table_name.to_string())
                                        .or_insert_with(|| Arc::new(DashMap::with_hasher(BuildHasherDefault::<AHasher>::default())))
                                        .insert(field.name.clone(), Arc::new(index.clone()));
                                }
                                if fulltext {
                                    // Разбиваем на слова — ищем по кусочкам!
                                    for word in value.split_whitespace() {
                                        index.entry(word.to_lowercase()).or_insert_with(Vec::new).push(row.id);
                                    }
                                    // Сохраняем для полного текста — словесный радар!
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

    // Применяем конфиг — база в курсе всех новостей!
    async fn apply_config(&self) {        
        let config = self.config.read().await; // Читаем настройки — что у нас в плане?
        // Проходим по таблицам — все на месте?
        for table_config in &config.tables {
            // Нет таблицы? Создаём — без паники!
            if !self.tables.contains_key(&table_config.name) {
                self.tables.insert(table_config.name.clone(), Arc::new(DashMap::with_hasher(BuildHasherDefault::<AHasher>::default())));                
                self.save_table(&table_config.name).await; // Сохраняем — на диск, чтобы не забыть!
            }            
            self.rebuild_indexes(&table_config.name).await; // Обновляем индексы — скорость наше всё!
        }
    }

    // Обновляем индексы — следим за порядком!
    async fn update_indexes(&self, table_name: &str, row: &Row, remove: bool) {
        let table_name = table_name.to_string(); // Имя в кармане!
        // Проходим по данным строки — что индексируем?
        for (field, value) in &row.data {
            // Обычные индексы — шустрые ярлыки!
            if let Some(index_map) = self.indexes.get(&table_name) {
                if let Some(index) = index_map.get(field) {
                    if remove {
                        // Удаляем ID — чистим следы!
                        if let Some(mut ids) = index.get_mut(value) { ids.retain(|&id| id != row.id); }
                    } else {
                        // Добавляем ID — метка на месте!
                        index.entry(value.clone()).or_insert_with(Vec::new).push(row.id);
                    }
                }
            }
            // Полнотекстовые — ищем по словам!
            if let Some(ft_index_map) = self.fulltext_indexes.get(&table_name) {
                if let Some(ft_index) = ft_index_map.get(field) {
                    // Разбиваем на слова — как детектив!
                    for word in value.split_whitespace() {
                        let word = word.to_lowercase();
                        if remove {
                            // Убираем ID — слово вне игры!
                            if let Some(mut ids) = ft_index.get_mut(&word) { ids.retain(|&id| id != row.id); }
                        } else {
                            // Добавляем ID — слово в деле!
                            ft_index.entry(word).or_insert_with(Vec::new).push(row.id);
                        }
                    }
                }
            }
        }
    }

    // Фильтруем строки — выцепляем нужное без лишнего!
    fn filter_rows(&self, table_name: &str, rows: Vec<Row>, where_clauses: &[Vec<Condition>]) -> Vec<Row> {
        let mut filtered = Vec::new(); // Новый список — чистый улов!
        // Проходим по группам условий — OR-группы в деле!
        for or_group in where_clauses {
            let mut group_rows = rows.clone(); // Копируем строки — работаем с запасом!
            // Фильтруем по условиям — точность наше всё!
            for condition in or_group {
                group_rows = match condition {
                    Condition::Eq(field, value) => self.index_filter(table_name, field, value, group_rows, |v, val| v == val), // Равно — бьём в точку!
                    Condition::Lt(field, value) => self.index_filter(table_name, field, value, group_rows, |v, val| v < val), // Меньше — отсекаем великанов!
                    Condition::Gt(field, value) => self.index_filter(table_name, field, value, group_rows, |v, val| v > val), // Больше — мелочь в сторону!
                    Condition::Contains(field, value) => self.fulltext_filter(table_name, field, value, group_rows), // Содержит — ищем тайники!
                    Condition::In(field, values) => group_rows.into_iter().filter(|r| r.data.get(field).map_or(false, |v| values.contains(v))).collect(), // В списке — по шпаргалке!
                    Condition::Between(field, min, max) => group_rows.into_iter().filter(|r| r.data.get(field).map_or(false, |v| v >= min && v <= max)).collect(), // Между — диапазон на глаз!
                };
            }
            filtered.extend(group_rows); // Добавляем в улов!
        }
        filtered.sort_by_key(|r| r.id); // Сортируем — порядок в хаосе!
        filtered.dedup_by_key(|r| r.id); // Убираем дубли — чистим сеть!
        filtered // Готово — чистый результат!
    }

    // Фильтр по индексам — скорость наше оружие!
    fn index_filter<F>(&self, table_name: &str, field: &str, value: &str, rows: Vec<Row>, pred: F) -> Vec<Row>
    where F: Fn(&str, &str) -> bool {
        // Проверяем индексы — есть ли шпаргалка?
        if let Some(index_map) = self.indexes.get(table_name) {
            if let Some(index) = index_map.get(field) {
                // Используем индекс — молниеносный поиск!
                return index.get(value).map_or(Vec::new(), |ids| {
                    ids.iter().filter_map(|id| self.tables.get(table_name).and_then(|t| t.get(id).map(|r| r.clone()))).collect()
                });
            }
        }
        // Нет индекса? Фильтруем вручную — без паники!
        rows.into_iter().filter(|r| r.data.get(field).map_or(false, |v| pred(v, value))).collect()
    }

    // Полнотекстовый фильтр — слова под микроскопом!
    fn fulltext_filter(&self, table_name: &str, field: &str, value: &str, rows: Vec<Row>) -> Vec<Row> {
        let value_lower = value.to_lowercase(); // Всё в нижний регистр — без капризов!
        // Проверяем полнотекст — есть ли словесный радар?
        if let Some(ft_index_map) = self.fulltext_indexes.get(table_name) {
            if let Some(ft_index) = ft_index_map.get(field) {
                let mut ids = Vec::new(); // Собираем ID — как улики!
                // Ищем слова — где спрятался запрос?
                for entry in ft_index.iter() {
                    if entry.key().contains(&value_lower) { ids.extend(entry.value().clone()); }
                }
                ids.sort_unstable(); // Сортируем — порядок в деле!
                ids.dedup(); // Дубли долой — чистим список!
                // Собираем строки — результат на блюде!
                return ids.into_iter().filter_map(|id| self.tables.get(table_name).and_then(|t| t.get(&id).map(|r| r.clone()))).collect();
            }
        }
        // Нет индекса? Ищем вручную — старый добрый способ!
        rows.into_iter().filter(|r| r.data.get(field).map_or(false, |v| v.to_lowercase().contains(&value_lower))).collect()
    }

    // Выполняем SELECT — добываем данные!
    async fn execute_select(&self, query: Query) -> Option<Vec<HashMap<String, String>>> {       
        let table = self.tables.get(&query.table)?;  // Берём таблицу
        // Собираем строки с псевдонимами — готовим базу!
        let rows: Vec<(String, Row)> = table.iter().map(|r| (query.alias.clone(), r.clone())).collect();
        // Начинаем с простого — каждая строка в своём наборе!
        let mut joined_rows: Vec<Vec<(String, Row)>> = rows.into_iter().map(|r| vec![r]).collect();
        // Джойним таблицы — связываем всё как профи!
        for (join_table, join_alias, on_left, on_right) in &query.joins {
            if let Some(join_table_data) = self.tables.get(join_table) {
                let left_field = on_left.split('.').nth(1).unwrap_or(on_left); // Левое поле — без лишних точек!
                let right_field = on_right.split('.').nth(1).unwrap_or(on_right); // Правое — тоже чистим!
                joined_rows = joined_rows.into_iter().filter_map(|mut row_set| {
                    let right_value = row_set[0].1.data.get(right_field); // Ищем связь справа!
                    join_table_data.iter()
                        .find(|jr| jr.data.get(left_field) == right_value) // Находим пару слева!
                        .map(|jr| {
                            row_set.push((join_alias.clone(), jr.clone())); // Добавляем в набор — готово!
                            row_set
                        })
                }).collect();
            }
        }

        // Фильтруем — отсекаем лишнее с умом!
        let filtered_rows = if !query.where_clauses.is_empty() {
            joined_rows.into_iter().filter(|row_set| {
                query.where_clauses.iter().any(|or_group| { // OR-группы — хоть что-то да сработает!
                    or_group.iter().all(|condition| { // AND внутри — всё должно совпасть!
                        match condition {
                            Condition::Eq(field, value) => { // Равно — точный удар!
                                let (alias, field_name) = field.split_once('.').unwrap_or(("", field));
                                let row = row_set.iter().find(|(a, _)| a == alias || (alias.is_empty() && a == &query.alias));
                                row.map_or(false, |(_, r)| r.data.get(field_name).map_or(false, |v| v == value))
                            }
                            Condition::Lt(field, value) => { // Меньше — мелочь в сторону!
                                let (alias, field_name) = field.split_once('.').unwrap_or(("", field));
                                let row = row_set.iter().find(|(a, _)| a == alias || (alias.is_empty() && a == &query.alias));
                                row.map_or(false, |(_, r)| r.data.get(field_name).map_or(false, |v| v < value))
                            }
                            Condition::Gt(field, value) => { // Больше — только крупняк!
                                let (alias, field_name) = field.split_once('.').unwrap_or(("", field));
                                let row = row_set.iter().find(|(a, _)| a == alias || (alias.is_empty() && a == &query.alias));
                                row.map_or(false, |(_, r)| r.data.get(field_name).map_or(false, |v| v > value))
                            }
                            Condition::Contains(field, value) => { // Содержит — ищем тайники!
                                let (alias, field_name) = field.split_once('.').unwrap_or(("", field));
                                let row = row_set.iter().find(|(a, _)| a == alias || (alias.is_empty() && a == &query.alias));
                                row.map_or(false, |(_, r)| r.data.get(field_name).map_or(false, |v| v.to_lowercase().contains(&value.to_lowercase())))
                            }
                            Condition::In(field, values) => { // В списке — по шпаргалке!
                                let (alias, field_name) = field.split_once('.').unwrap_or(("", field));
                                let row = row_set.iter().find(|(a, _)| a == alias || (alias.is_empty() && a == &query.alias));
                                row.map_or(false, |(_, r)| r.data.get(field_name).map_or(false, |v| values.contains(v)))
                            }
                            Condition::Between(field, min, max) => { // Между — диапазон на глаз!
                                let (alias, field_name) = field.split_once('.').unwrap_or(("", field));
                                let row = row_set.iter().find(|(a, _)| a == alias || (alias.is_empty() && a == &query.alias));
                                row.map_or(false, |(_, r)| r.data.get(field_name).map_or(false, |v| v >= min && v <= max))
                            }
                        }
                    })
                })
            }).collect()
        } else {
            joined_rows // Без фильтров? Берём всё!
        };

        // Формируем результат — красиво и по полочкам!
        let results: Vec<_> = filtered_rows.into_iter().map(|row_set| {
            let mut result = HashMap::new();
            for (alias, row) in row_set {
                for field in &query.fields {
                    if field == "*" { // Всё? Гребём лопатой!
                        for (k, v) in &row.data { result.insert(format!("{}.{}", alias, k), v.clone()); }
                    } else if field.contains('.') { // Точка? Целимся точно!
                        let (field_alias, field_name) = field.split_once('.').unwrap();
                        if field_alias == alias { row.data.get(field_name).map(|v| result.insert(field.clone(), v.clone())); }
                    } else if query.joins.is_empty() { // Без джойнов? Просто берём!
                        row.data.get(field).map(|v| result.insert(field.clone(), v.clone()));
                    }
                }
            }
            result
        }).collect();

        // Пусто? None! Есть добыча? Some!
        if results.is_empty() { None } else { Some(results) }
    }

    // Вставляем данные — новый груз в базу!
    async fn execute_insert(&self, query: Query) {        
        let autoincrement_field = self.get_autoincrement_field(&query.table).await; // Ищем автоинкремент — кто считает ID?
        // Берём или создаём таблицу — место для новенького!
        let table_data = self.tables.entry(query.table.clone())
            .or_insert_with(|| Arc::new(DashMap::with_hasher(BuildHasherDefault::<AHasher>::default())))
            .clone();

        // Проходим по значениям — кидаем всё в кучу!
        for mut query_values in query.values {
            let mut id = if let Some(field) = &autoincrement_field {
                if let Some(value) = query_values.get(field) {
                    value.parse::<i32>().unwrap_or_else(|_| {
                        table_data.iter().map(|r| r.id).max().unwrap_or(0) + 1 // Новый ID — следующий в очереди!
                    })
                } else {
                    table_data.iter().map(|r| r.id).max().unwrap_or(0) + 1 // Нет значения? Считаем сами!
                }
            } else {
                table_data.iter().map(|r| r.id).max().unwrap_or(0) + 1 // Без авто? Всё равно новый!
            };

            // Проверяем ID — никаких повторов!
            while table_data.contains_key(&id) { id += 1; }

            // Добавляем ID в значения — порядок в доме!
            if let Some(field) = &autoincrement_field {
                query_values.insert(field.clone(), id.to_string());
            }

            let row = Row { id, data: query_values }; // Новая строка — свежий улов!
            table_data.insert(row.id, row.clone()); // Кидаем в таблицу!
            self.update_indexes(&query.table, &row, false).await; // Обновляем индексы — метки на месте!
        }
        self.save_table(&query.table).await; // Сохраняем — на диск без промедления!
        self.join_cache.retain(|key, _| !key.contains(&query.table)); // Чистим кэш — старое долой!
    }

    // Обновляем данные — подкручиваем гайки!
    async fn execute_update(&self, query: Query) {
        // Берём таблицу — есть ли что добавить?
        if let Some(table) = self.tables.get(&query.table) {
            // Собираем строки — полный список 
            let mut to_update = table.iter().map(|r| r.clone()).collect::<Vec<_>>();
            // Фильтруем, если есть условия — только нужное
            if !query.where_clauses.is_empty() {
                to_update = self.filter_rows(&query.table, to_update, &query.where_clauses);
            }

            // Есть что обновить? Вперёд!
            if let Some(update_values) = query.values.first() {
                for mut row in to_update {
                    self.update_indexes(&query.table, &row, true).await; // Убираем старые метки!
                    row.data.extend(update_values.clone()); // Добавляем новые данные — апгрейд!
                    self.update_indexes(&query.table, &row, false).await; // Новые метки — готово!
                    table.insert(row.id, row); // Обновляем таблицу!
                }
                self.save_table(&query.table).await; // Сохраняем — всё в деле!
                self.join_cache.retain(|key, _| !key.contains(&query.table)); // Чистим кэш — без хлама!
            }
        }
    }

    // Удаляем данные — чистим базу от лишнего!
    async fn execute_delete(&self, query: Query) {
        // Есть таблица? Убираем ненужное!
        if let Some(table) = self.tables.get(&query.table) {
            // Фильтруем строки — что под нож?
            let to_delete = self.filter_rows(&query.table, table.iter().map(|r| r.clone()).collect(), &query.where_clauses);
            for row in to_delete {
                self.update_indexes(&query.table, &row, true).await; // Убираем метки — следов не будет!
                table.remove(&row.id); // Удаляем строку — чистота!
            }
            self.save_table(&query.table).await; // Сохраняем — порядок на диске!
            self.join_cache.retain(|key, _| !key.contains(&query.table)); // Чистим кэш — без остатков!
        }
    }
}
