use std::collections::HashMap; // Ключи и значения — пиратская карта добычи!
use std::sync::Arc; // Делимся сокровищами между потоками — надёжно!
use std::hash::BuildHasherDefault; // Хэш-функция — замок с хитрым ключом!
use ahash::AHasher; // Быстрый хэшер — как молния в ночи!
use dashmap::DashMap; // Турбо-карта — быстрая, многопоточная, без багов!
use serde::{Serialize, Deserialize}; // Магия превращения данных в байты и обратно!
use toml; // Парсер TOML — читаем пиратские карты!
use tokio::fs::{File, create_dir_all, OpenOptions}; // Асинхронная работа с сундуками на диске!
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter}; // Читаем и пишем байты — шустро!
use tokio::sync::{RwLock, Mutex}; // Замок для сокровищ — один пишет, другие ждут!
use tokio::time::{sleep, Duration, interval}; // Таймеры — ждём момент для атаки!
use std::path::Path; // Путь к сокровищам — карта в руках!
use bincode; // Сериализация — превращаем добычу в байты!

type Hasher = BuildHasherDefault<AHasher>; // Хэшер — наш верный помощник!

// Типы данных — золото, ром или карты? Теперь знаем точно!
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum Value {
    Numeric(f64),   // Числа — подсчитываем дублоны!
    Text(String),   // Текст — имена пиратов и названия кораблей!
    Timestamp(i64), // Время — когда подняли чёрный флаг!
    Boolean(bool),  // Да/Нет — есть ли ром в трюме?
}

impl Value {
    // Превращаем сокровище в строку — для карты или вывода!
    fn to_string(&self) -> String {
        match self {
            Value::Numeric(n) => n.to_string(),
            Value::Text(s) => s.clone(),
            Value::Timestamp(t) => t.to_string(),
            Value::Boolean(b) => b.to_string(),
        }
    }
}

// Конфиг базы — наш план сокровищ!
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DbConfig {
    tables: Vec<TableConfig>, // Таблицы — сундуки с добычей!
}

// Пустой конфиг — если всё сломалось, начнём заново!
impl Default for DbConfig {
    fn default() -> Self {
        Self { tables: Vec::new() } // Пустой трюм — начинаем с нуля!
    }
}

// Описание сундука — что внутри?
#[derive(Debug, Serialize, Deserialize, Clone)]
struct TableConfig {
    name: String,              // Имя сундука — коротко и ясно!
    fields: Vec<FieldConfig>, // Что прячем внутри?
}

// Поля — что за клад и как его искать!
#[derive(Debug, Serialize, Deserialize, Clone)]
struct FieldConfig {
    name: String,           // Название клада!
    field_type: String,     // Тип: "numeric", "text", "timestamp", "boolean" — что за добро?
    indexed: Option<bool>,  // Индекс — шустрый поиск!
    fulltext: Option<bool>, // Полнотекст — ищем по словам!
    unique: Option<bool>,   // Уникальность — только один такой!
    autoincrement: Option<bool>, // Авто-ID — для новых пиратов!
}

// Строка — кусочек добычи с ID и типами!
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Row {
    pub id: i32,                    // ID — номер пирата в команде!
    pub data: HashMap<String, Value>, // Данные — сундук с разным добром!
}

// Условия — как выцепить нужный клад!
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Condition {
    Eq(String, String),         // Равно — точный удар!
    Lt(String, String),         // Меньше — мелочь в сторону!
    Gt(String, String),         // Больше — только крупняк!
    Contains(String, String),   // Содержит — ищем тайники!
    In(String, Vec<String>),    // В списке — по шпаргалке!
    Between(String, String, String), // Между — диапазон на глаз!
}

// Write-Ahead Logging (WAL) — журнал операций для целостности данных!
// Операции сначала записываются сюда, а затем применяются к основным данным.
// Это позволяет восстановить базу в случае сбоя!
#[derive(Debug, Serialize, Deserialize)]
enum WalOperation {
    Insert {
        table: String,                    // Имя сундука — куда грузим!
        values: Vec<HashMap<String, String>>, // Добыча — что кладём!
    },
    Update {
        table: String,                    // Имя сундука — где правим!
        values: HashMap<String, String>,  // Новые ценности — что меняем!
        where_clauses: Vec<Vec<Condition>>, // Условия — что трогаем!
    },
    Delete {
        table: String,                    // Имя сундука — откуда убираем!
        where_clauses: Vec<Vec<Condition>>, // Условия — что выкидываем!
    },
}

// База — наш корабль с сокровищами!
#[derive(Clone)]
pub struct Database {
    tables: Arc<DashMap<String, Arc<DashMap<i32, Row, Hasher>>, Hasher>>, // Таблицы — трюмы с добычей!
    indexes: Arc<DashMap<String, Arc<DashMap<String, Arc<DashMap<String, Vec<i32>, Hasher>>, Hasher>>, Hasher>>, // Индексы — шустрые метки!
    fulltext_indexes: Arc<DashMap<String, Arc<DashMap<String, Arc<DashMap<String, Vec<i32>, Hasher>>, Hasher>>, Hasher>>, // Полнотекст — словесный радар!
    data_dir: String,           // Папка — наш тайник на берегу!
    config_file: String,        // Карта — где всё спрятано!
    join_cache: Arc<DashMap<String, Vec<(Row, Row)>, Hasher>>, // Кэш связок — быстрый доступ к флоту!
    config: Arc<RwLock<DbConfig>>, // Конфиг с замком — безопасность на уровне!
    wal_file: Arc<Mutex<BufWriter<File>>>, // WAL-файл — журнал для надёжности!
}

// Запрос — наш план захвата добычи!
#[derive(Debug, Clone)]
pub struct Query {
    pub table: String,                    // Куда лезем за сокровищами?
    pub fields: Vec<String>,             // Что берём из сундука?
    pub alias: String,                   // Прозвище — чтобы не спутать!
    pub joins: Vec<(String, String, String, String)>, // Связи — собираем флот!
    pub where_clauses: Vec<Vec<Condition>>, // Условия — отсекаем лишних!
    pub values: Vec<HashMap<String, String>>, // Добыча для вставки!
    pub op: QueryOp,                     // Что делаем — грабим или смотрим?
    pub order_by: Option<(String, bool)>, // Сортировка — порядок в трюме! ASC=true, DESC=false
    pub group_by: Option<String>,         // Группировка — считаем добычу по кучам!
    pub limit: Option<usize>,            // Лимит — сколько сокровищ утащить с корабля?
    pub offset: Option<usize>,           // Смещение — с какого дублона начинаем грабёж?
}

// Тип операции — команда для базы, коротко и чётко!
#[derive(Debug, Clone, Default)]
pub enum QueryOp {
    #[default]
    Select,  // Смотрим добычу!
    Insert,  // Грузим в трюм!
    Update,  // Меняем ром на золото!
    Delete,  // Выкидываем за борт!
}

// Явная реализация Default для Query — задаём начальные значения!
impl Default for Query {
    fn default() -> Self {
        Self {
            table: String::new(),               // Пустой трюм — пока не выбрали!
            fields: vec!["*".to_string()],      // Берём всё — жадность побеждает!
            alias: String::new(),               // Без клички — инкогнито!
            joins: Vec::new(),                  // Без флота — одиночки!
            where_clauses: Vec::new(),          // Без фильтров — всё в кучу!
            values: Vec::new(),                 // Пустой сундук — ждём добычу!
            op: QueryOp::Select,                // По умолчанию смотрим — любопытство!
            order_by: None,                     // Хаос в трюме — без порядка!
            group_by: None,                     // Без кучек — всё вперемешку!
            limit: None,                        // Без лимита — тащим всё, что найдём!
            offset: None,                       // Без смещения — начинаем с первого клада!
        }
    }
}

// Макрос для сборки запросов — автоматика в деле!
macro_rules! query_builder {
    ($method:ident, $op:ident) => {
        // Метод-запускатор: берём таблицу и готовим запрос!
        pub fn $method(&self, table: &str) -> Query {
            Query {
                table: table.to_string(),       // Куда плывём?
                alias: table.to_string(),       // Кличка по умолчанию!
                op: QueryOp::$op,              // Что делаем?
                fields: vec!["*".to_string()], // Хватаем всё!
                ..Default::default()           // Остальное — по нулям!
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
        self // Хватаем добычу и плывём дальше!
    }
    // Псевдоним 
    pub fn alias(mut self, alias: &str) -> Self {
        self.alias = alias.to_string();
        self // Даём кличку и вперёд!
    }
    // Джоин — связываем таблицы, как конструктор!
    pub fn join(mut self, table: &str, alias: &str, on_left: &str, on_right: &str) -> Self {
        self.joins.push((table.to_string(), alias.to_string(), on_left.to_string(), on_right.to_string()));
        self // Связываем флот и плывём!
    }
    // Значения — кидаем данные в запрос, без лишних рук!
    pub fn values<V>(mut self, values: V) -> Self where V: IntoValues {
        self.values = values.into_values();
        self // Грузим сундук и дальше!
    }
    // Условия — фильтры для точных ударов!
    add_condition!(where_eq, Eq);     // Точный удар!
    add_condition!(where_lt, Lt);     // Мелочь в сторону!
    add_condition!(where_gt, Gt);     // Только крупняк!
    add_condition!(where_contains, Contains); // Ищем тайники!

    // Где "в списке" — проверка по шпаргалке!
    pub fn where_in<T: Into<String>>(mut self, field: &str, values: Vec<T>) -> Self {
        if self.where_clauses.is_empty() { self.where_clauses.push(Vec::new()); }
        self.where_clauses.last_mut().unwrap().push(Condition::In(field.to_string(), values.into_iter().map(Into::into).collect()));
        self // По шпаргалке и дальше!
    }

    // Где "между" — диапазон для умников!
    pub fn where_between<T: Into<String>>(mut self, field: &str, min: T, max: T) -> Self {
        if self.where_clauses.is_empty() { self.where_clauses.push(Vec::new()); }
        self.where_clauses.last_mut().unwrap().push(Condition::Between(field.to_string(), min.into(), max.into()));
        self // Диапазон и вперёд!
    }

    // Сортировка — порядок в трюме, ASC или DESC!
    pub fn order_by(mut self, field: &str, ascending: bool) -> Self {
        self.order_by = Some((field.to_string(), ascending)); // Поле и порядок: ASC=true, DESC=false — всё под контролем!
        self // Цепочка — наш герой!
    }

    // Группировка — делим добычу по кучкам!
    pub fn group_by(mut self, field: &str) -> Self {
        self.group_by = Some(field.to_string()); // Считаем добычу по полям — порядок в хаосе!
        self // Цепочка не рвётся!
    }

    // Лимит — сколько добычи утащить с корабля!
    pub fn limit(mut self, count: usize) -> Self {
        self.limit = Some(count); // Устанавливаем лимит — не больше этого в сундук!
        self // Цепочка — плывём дальше!
    }

    // Смещение — с какого дублона начинаем грабёж!
    pub fn offset(mut self, start: usize) -> Self {
        self.offset = Some(start); // Устанавливаем смещение — пропускаем первые сокровища!
        self // Цепочка — на абордаж!
    }

    // Выполняем запрос — время жать на кнопку!
    pub async fn execute(self, db: &Database) -> Option<Vec<HashMap<String, String>>> {
        match self.op {
            QueryOp::Select => db.execute_select(self).await, // Читаем добычу!
            QueryOp::Insert => {
                // Записываем операцию в WAL — безопасность прежде всего!
                let operation = WalOperation::Insert {
                    table: self.table.clone(),
                    values: self.values.clone(),
                };
                db.log_to_wal(&operation).await;
                db.execute_insert(self).await; // Грузим в трюм!
                None
            }
            QueryOp::Update => {
                if let Some(values) = self.values.first() {
                    // Записываем операцию в WAL — фиксируем изменения!
                    let operation = WalOperation::Update {
                        table: self.table.clone(),
                        values: values.clone(),
                        where_clauses: self.where_clauses.clone(),
                    };
                    db.log_to_wal(&operation).await;
                    db.execute_update(self).await; // Меняем ром на золото!
                }
                None
            }
            QueryOp::Delete => {
                // Записываем операцию в WAL — убираем с гарантией!
                let operation = WalOperation::Delete {
                    table: self.table.clone(),
                    where_clauses: self.where_clauses.clone(),
                };
                db.log_to_wal(&operation).await;
                db.execute_delete(self).await; // Выкидываем за борт!
                None
            }
        }
    }
}

// "Пульт управления" — база в наших руках!
impl Database {
    // Создаём базу — как собрать корабль с нуля!
    pub async fn new(data_dir: &str, config_file: &str) -> Self {
        // Читаем конфиг — вдруг там секрет успеха!
        let config_str = tokio::fs::read_to_string(config_file).await.unwrap_or_default();
        // Парсим настройки и прячем под замок!
        let config = Arc::new(RwLock::new(toml::from_str(&config_str).unwrap_or_default()));
        // Открываем WAL-файл — журнал для операций!
        let wal_path = format!("{}/wal.log", data_dir);
        let wal_file = OpenOptions::new()
			.create(true)
			.append(true)
			.open(&wal_path)
			.await
			.unwrap();
        let wal_writer = BufWriter::new(wal_file);
        let wal_file = Arc::new(Mutex::new(wal_writer));
        // Собираем корабль — все трюмы на месте!
        let db = Self {
            tables: Arc::new(DashMap::with_hasher(BuildHasherDefault::<AHasher>::default())), // Таблицы — наш склад!
            indexes: Arc::new(DashMap::with_hasher(BuildHasherDefault::<AHasher>::default())), // Индексы — шустрые ярлыки!
            fulltext_indexes: Arc::new(DashMap::with_hasher(BuildHasherDefault::<AHasher>::default())), // Полнотекст — словесный радар!
            data_dir: data_dir.to_string(),       // Папка — наш тайник!
            config_file: config_file.to_string(), // Файл конфига — карта!
            join_cache: Arc::new(DashMap::with_hasher(BuildHasherDefault::<AHasher>::default())), // Кэш — ускорение в кармане!
            config,                              // Конфиг с замком — надёжно!
            wal_file,                            // WAL — наш страховочный трос!
        };
        // Создаём тайник для данных — копаем яму!
        create_dir_all(data_dir).await.unwrap_or(());
        // Загружаем добычу с диска — оживляем корабль!
        db.load_tables_from_disk().await;
        // Восстанавливаем из WAL — если есть несохранённые операции!
        db.recover_from_wal().await;
        // Клонируем и запускаем шпиона за картой — следим за изменениями!
        let db_clone = db.clone();
        tokio::spawn(async move { db_clone.watch_config().await });
        // Запускаем фоновую задачу для сброса WAL в основной файл каждые 60 секунд!
        let db_flush = db.clone();
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                db_flush.flush_wal_to_bin().await;
            }
        });
        db // Готово — корабль на плаву!
    }

    // Запускаторы запросов — штурвал в руках!
    query_builder!(select, Select); // Читаем добычу!
    query_builder!(insert, Insert); // Грузим в трюм!
    query_builder!(update, Update); // Меняем ром на золото!
    query_builder!(delete, Delete); // Выкидываем за борт!

    // Записываем операцию в WAL — фиксируем намерения!
    async fn log_to_wal(&self, operation: &WalOperation) {
        let mut wal = self.wal_file.lock().await; // Захватываем журнал!
        let encoded = bincode::serialize(operation).unwrap(); // Кодируем операцию!
        wal.write_all(&encoded).await.unwrap(); // Пишем в WAL!
        wal.flush().await.unwrap(); // Убеждаемся, что всё на диске!
    }

    // Восстанавливаем из WAL — спасаем добычу после шторма!
    async fn recover_from_wal(&self) {
        let wal_path = format!("{}/wal.log", self.data_dir);
        if !Path::new(&wal_path).exists() { return; } // Нет WAL? Нечего восстанавливать!
        let file = File::open(&wal_path).await.unwrap(); // Открываем журнал!
        let mut reader = BufReader::new(file);
        let mut buffer = Vec::new(); // Буфер для чтения!
        reader.read_to_end(&mut buffer).await.unwrap();
        if buffer.is_empty() { return; } // Пустой WAL? Выходим!
        let operations: Vec<WalOperation> = bincode::deserialize(&buffer).unwrap_or_default(); // Читаем операции!
        for op in operations {
            match op {
                WalOperation::Insert { table, values } => {
                    let query = Query {
                        table,
                        op: QueryOp::Insert,
                        values,
                        ..Default::default()
                    };
                    self.execute_insert(query).await; // Применяем вставку!
                }
                WalOperation::Update { table, values, where_clauses } => {
                    let query = Query {
                        table,
                        op: QueryOp::Update,
                        values: vec![values],
                        where_clauses,
                        ..Default::default()
                    };
                    self.execute_update(query).await; // Применяем обновление!
                }
                WalOperation::Delete { table, where_clauses } => {
                    let query = Query {
                        table,
                        op: QueryOp::Delete,
                        where_clauses,
                        ..Default::default()
                    };
                    self.execute_delete(query).await; // Применяем удаление!
                }
            }
        }
        // Очищаем WAL после восстановления!
        File::create(&wal_path).await.unwrap();
    }

    // Сбрасываем WAL в основной файл — сохраняем порядок!
    async fn flush_wal_to_bin(&self) {
        // Сохраняем все таблицы в .bin!
        for table_name in self.tables.iter().map(|t| t.key().clone()).collect::<Vec<_>>() {
            self.save_table(&table_name).await;
        }
        // Очищаем WAL — всё синхронизировано!
        let wal_path = format!("{}/wal.log", self.data_dir);
        File::create(&wal_path).await.unwrap();
    }

    // Ищем уникальное поле — кто тут особый?
    async fn get_unique_field(&self, table_name: &str) -> Option<String> {
        self.config.read().await.tables.iter()
            .find(|t| t.name == table_name) // Находим сундук!
            .and_then(|t| t.fields.iter().find(|f| f.unique.unwrap_or(false)).map(|f| f.name.clone())) // Выцепляем уникальный клад!
    }

    // Ищем автоинкремент — кто сам считает?
    async fn get_autoincrement_field(&self, table_name: &str) -> Option<String> {
        self.config.read().await.tables.iter()
            .find(|t| t.name == table_name) // Ищем сундук — где автоматика?
            .and_then(|t| t.fields.iter().find(|f| f.autoincrement.unwrap_or(false)).map(|f| f.name.clone())) // Хватаем поле с авто-ID!
    }

    // Шпион следит за картой — глаз не спускает!
    async fn watch_config(&self) {
        let mut last_content = String::new(); // Старая карта — чистый ноль!
        loop {
            if let Ok(content) = tokio::fs::read_to_string(&self.config_file).await { // Читаем карту — что нового?
                if content != last_content { // Изменилась? Пора действовать!
                    *self.config.write().await = toml::from_str(&content).unwrap_or_default(); // Обновляем — свежий план!
                    self.apply_config().await; // Применяем — корабль в тонусе!
                    last_content = content; // Запоминаем — теперь это наша карта!
                }
            }
            sleep(Duration::from_secs(5)).await; // Ждём 5 сек — отдых для шпиона!
        }
    }

    // Грузим добычу с диска — оживаем корабль!
    async fn load_tables_from_disk(&self) {
        // Читаем тайник — где наш склад?
        if let Ok(mut entries) = tokio::fs::read_dir(&self.data_dir).await {
            // Проходим по сундукам — что тут у нас?
            while let Ok(Some(entry)) = entries.next_entry().await {
                // Берём только .bin — остальное не трогаем!
                if entry.path().extension() == Some("bin".as_ref()) {
                    // Имя сундука — выдираем из файла!
                    let table_name = entry.path().file_stem().unwrap().to_str().unwrap().to_string();
                    // Открываем сундук — лезем в закрома!
                    let mut file = File::open(&entry.path()).await.unwrap();
                    let mut buffer = Vec::new(); // Буфер — наш временный ящик!
                    file.read_to_end(&mut buffer).await.unwrap(); // Читаем всё — до последнего дублона!
                    // Распаковываем добычу — сокровища в руках!
                    if let Ok(rows) = bincode::deserialize::<HashMap<i32, Row>>(&buffer) {
                        // Новый трюм — свежий контейнер!
                        let table = Arc::new(DashMap::with_hasher(BuildHasherDefault::<AHasher>::default()));
                        // Ищем уникальный клад — кто тут особый?
                        let unique_field = self.get_unique_field(&table_name).await;
                        let mut seen = std::collections::HashSet::new(); // Список виденного — дубли в бан!
                        // Проходим по добыче — грузим добро!
                        for (id, row) in rows {
                            if let Some(ref field) = unique_field {
                                if let Some(value) = row.data.get(field) {
                                    if seen.contains(&value.to_string()) { continue; } // Повтор? Пропускаем!
                                    seen.insert(value.to_string()); // Новое — записываем!
                                }
                            }
                            table.insert(id, row); // Кидаем в трюм — порядок!
                        }
                        // Сохраняем трюм — место занято!
                        self.tables.insert(table_name.clone(), table);
                        // Перестраиваем метки — ускоряем поиск!
                        self.rebuild_indexes(&table_name).await;
                    }
                }
            }
        }
    }

    // Сохраняем трюм — прячем добычу на диск!
    async fn save_table(&self, table_name: &str) {
        // Берём трюм — есть ли что спасать?
        if let Some(table) = self.tables.get(table_name) {
            let path = format!("{}/{}.bin", self.data_dir, table_name); // Путь для сундука — наш цифровой сейф!
            let rows: HashMap<i32, Row> = table.iter().map(|r| (*r.key(), r.value().clone())).collect(); // Собираем добычу — всё в кучу!
            let encoded = bincode::serialize(&rows).unwrap(); // Кодируем — превращаем в байты!
            File::create(&path).await.unwrap().write_all(&encoded).await.unwrap(); // Пишем на диск — теперь не пропадёт!
        }
    }

    // Перестраиваем метки — ускоряем корабль до турбо-режима!
    async fn rebuild_indexes(&self, table_name: &str) {
        // Проверяем трюм — есть ли что индексировать?
        if let Some(table) = self.tables.get(table_name) {
            let config = self.config.read().await; // Читаем карту — где наши настройки?
            // Ищем сундук в карте — кто тут главный?
            if let Some(table_config) = config.tables.iter().find(|t| t.name == table_name) {
                // Проходим по кладам — какие ускоряем?
                for field in &table_config.fields {
                    let (indexed, fulltext) = (field.indexed.unwrap_or(false), field.fulltext.unwrap_or(false));
                    // Если клад индексируемый — вперёд!
                    if indexed || fulltext {
                        // Новая метка — чистый лист для скорости!
                        let index = DashMap::with_hasher(BuildHasherDefault::<AHasher>::default());
                        // Проходим по добыче — собираем ярлыки!
                        for row in table.iter() {
                            if let Some(value) = row.data.get(&field.name) {
                                if indexed {
                                    // Добавляем в метку — шустрый поиск!
                                    index.entry(value.to_string()).or_insert_with(Vec::new).push(row.id);
                                    // Кидаем в общий список — порядок в доме!
                                    self.indexes.entry(table_name.to_string())
                                        .or_insert_with(|| Arc::new(DashMap::with_hasher(BuildHasherDefault::<AHasher>::default())))
                                        .insert(field.name.clone(), Arc::new(index.clone()));
                                }
                                if fulltext {
                                    // Разбиваем на слова — ищем по кусочкам!
                                    for word in value.to_string().split_whitespace() {
                                        index.entry(word.to_lowercase()).or_insert_with(Vec::new).push(row.id);
                                    }
                                    // Сохраняем для словесного радара — всё под контролем!
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

    // Применяем карту — корабль в курсе всех новостей!
    async fn apply_config(&self) {
        let config = self.config.read().await; // Читаем настройки — что у нас в плане?
        // Проходим по сундукам — все на месте?
        for table_config in &config.tables {
            // Нет трюма? Создаём — без паники!
            if !self.tables.contains_key(&table_config.name) {
                self.tables.insert(table_config.name.clone(), Arc::new(DashMap::with_hasher(BuildHasherDefault::<AHasher>::default())));
                self.save_table(&table_config.name).await; // Сохраняем — на диск, чтобы не забыть!
            }
            self.rebuild_indexes(&table_config.name).await; // Обновляем метки — скорость наше всё!
        }
    }

    // Обновляем метки — следим за порядком!
    async fn update_indexes(&self, table_name: &str, row: &Row, remove: bool) {
        let table_name = table_name.to_string(); // Имя в кармане!
        // Проходим по добыче строки — что индексируем?
        for (field, value) in &row.data {
            // Обычные метки — шустрые ярлыки!
            if let Some(index_map) = self.indexes.get(&table_name) {
                if let Some(index) = index_map.get(field) {
                    if remove {
                        // Удаляем ID — чистим следы!
                        if let Some(mut ids) = index.get_mut(&value.to_string()) { ids.retain(|&id| id != row.id); }
                    } else {
                        // Добавляем ID — метка на месте!
                        index.entry(value.to_string()).or_insert_with(Vec::new).push(row.id);
                    }
                }
            }
            // Полнотекстовые — ищем по словам!
            if let Some(ft_index_map) = self.fulltext_indexes.get(&table_name) {
                if let Some(ft_index) = ft_index_map.get(field) {
                    // Разбиваем на слова — как детектив!
                    for word in value.to_string().split_whitespace() {
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

    // Фильтруем добычу — выцепляем нужное без лишнего!
    async fn filter_rows(&self, table_name: &str, rows: Vec<Row>, where_clauses: &[Vec<Condition>]) -> Vec<Row> {
        let mut filtered = Vec::new(); // Новый список — чистый улов!
        // Проходим по группам условий — OR-группы в деле!
        for or_group in where_clauses {
            let mut group_rows = rows.clone(); // Копируем добычу — работаем с запасом!
            // Фильтруем по условиям — точность наше всё!
            for condition in or_group {
                group_rows = match condition {
                    Condition::Eq(field, value) => self.index_filter(table_name, field, value, group_rows, |v, val| v.to_string() == val), // Равно — бьём в точку!
                    Condition::Lt(field, value) => self.index_filter(table_name, field, value, group_rows, |v, val| v.to_string().as_str() < val), // Меньше — отсекаем великанов!
                    Condition::Gt(field, value) => self.index_filter(table_name, field, value, group_rows, |v, val| v.to_string().as_str() > val), // Больше — мелочь в сторону!
                    Condition::Contains(field, value) => self.fulltext_filter(table_name, field, value, group_rows), // Содержит — ищем тайники!
                    Condition::In(field, values) => group_rows.into_iter().filter(|r| r.data.get(field).map_or(false, |v| values.contains(&v.to_string()))).collect(), // В списке — по шпаргалке!
                    Condition::Between(field, min, max) => group_rows.into_iter().filter(|r| {
                        r.data.get(field).map_or(false, |v| {
                            let v_str = v.to_string();
                            v_str >= *min && v_str <= *max
                        })
                    }).collect(), // Между — диапазон на глаз!
                };
            }
            filtered.extend(group_rows); // Добавляем в улов!
        }
        filtered.dedup_by_key(|r| r.id); // Убираем дубли — чистим сеть!
        filtered // Готово — чистый результат!
    }

    // Фильтр по меткам — скорость наше оружие!
    fn index_filter<F>(&self, table_name: &str, field: &str, value: &str, rows: Vec<Row>, pred: F) -> Vec<Row>
    where F: Fn(&Value, &str) -> bool {
        // Проверяем метки — есть ли шпаргалка?
        if let Some(index_map) = self.indexes.get(table_name) {
            if let Some(index) = index_map.get(field) {
                // Используем метку — молниеносный поиск!
                return index.get(value).map_or(Vec::new(), |ids| {
                    ids.iter().filter_map(|id| self.tables.get(table_name).and_then(|t| t.get(id).map(|r| r.clone()))).collect()
                });
            }
        }
        // Нет метки? Фильтруем вручную — без паники!
        rows.into_iter().filter(|r| r.data.get(field).map_or(false, |v| pred(v, value))).collect()
    }

    // Полнотекстовый фильтр — слова под микроскопом!
    fn fulltext_filter(&self, table_name: &str, field: &str, value: &str, rows: Vec<Row>) -> Vec<Row> {
        let value_lower = value.to_lowercase(); // Всё в нижний регистр — без капризов!
        // Проверяем словесный радар — есть ли что найти?
        if let Some(ft_index_map) = self.fulltext_indexes.get(table_name) {
            if let Some(ft_index) = ft_index_map.get(field) {
                let mut ids = Vec::new(); // Собираем ID — как улики!
                // Ищем слова — где спрятался запрос?
                for entry in ft_index.iter() {
                    if entry.key().contains(&value_lower) { ids.extend(entry.value().clone()); }
                }
                ids.sort_unstable(); // Сортируем — порядок в деле!
                ids.dedup(); // Дубли долой — чистим список!
                // Собираем добычу — результат на блюде!
                return ids.into_iter().filter_map(|id| self.tables.get(table_name).and_then(|t| t.get(&id).map(|r| r.clone()))).collect();
            }
        }
        // Нет радара? Ищем вручную — старый добрый способ!
        rows.into_iter().filter(|r| r.data.get(field).map_or(false, |v| v.to_string().to_lowercase().contains(&value_lower))).collect()
    }

    // Выполняем SELECT — добываем сокровища!
    async fn execute_select(&self, query: Query) -> Option<Vec<HashMap<String, String>>> {
        let table = self.tables.get(&query.table)?; // Берём сундук — где добыча?
        // Собираем добычу с кличками — готовим базу!
        let rows: Vec<(String, Row)> = table.iter().map(|r| (query.alias.clone(), r.clone())).collect();
        // Начинаем с простого — каждая добыча в своём наборе!
        let mut joined_rows: Vec<Vec<(String, Row)>> = rows.into_iter().map(|r| vec![r]).collect();
        // Джойним флот — связываем всё как профи!
        for (join_table, join_alias, on_left, on_right) in &query.joins {
            if let Some(join_table_data) = self.tables.get(join_table) {
                let left_field = on_left.split('.').nth(1).unwrap_or(on_left); // Левое поле — без лишних точек!
                let right_field = on_right.split('.').nth(1).unwrap_or(on_right); // Правое — тоже чистим!
                joined_rows = joined_rows.into_iter().filter_map(|mut row_set| {
                    let right_value = row_set[0].1.data.get(right_field).map(|v| v.to_string()); // Ищем связь справа!
                    join_table_data.iter()
                        .find(|jr| jr.data.get(left_field).map(|v| v.to_string()) == right_value) // Находим пару слева!
                        .map(|jr| {
                            row_set.push((join_alias.clone(), jr.clone())); // Добавляем в набор — флот готов!
                            row_set
                        })
                }).collect();
            }
        }

        // Фильтруем добычу основной таблицы — отсекаем лишнее с умом!
        let filtered_rows = if !query.where_clauses.is_empty() {
            self.filter_rows(&query.table, joined_rows.iter().map(|r| r[0].1.clone()).collect::<Vec<Row>>(), &query.where_clauses).await
        } else {
            joined_rows.iter().map(|r| r[0].1.clone()).collect::<Vec<Row>>()
        };

        // Оставляем только нужные наборы — чистим флот по ID!
        let filtered_ids: std::collections::HashSet<i32> = filtered_rows.into_iter().map(|r| r.id).collect();
        joined_rows.retain(|row_set| filtered_ids.contains(&row_set[0].1.id));

        let config = self.config.read().await; // Читаем карту — где порядок?

        // Применяем сортировку — раскладываем добычу по полочкам!
        if let Some((field, ascending)) = &query.order_by {
            let (alias, field_name) = field.split_once('.').unwrap_or(("", field)); // Разделяем кличку и клад!
            let table_name = if alias.is_empty() || alias == query.alias {
                &query.table // Основной сундук — наш корабль!
            } else {
                query.joins.iter().find(|(_, a, _, _)| a == alias).map(|(t, _, _, _)| t).unwrap_or(&query.table) // Ищем союзника во флоте!
            };
            let table_config = config.tables.iter().find(|t| t.name == *table_name); // Находим сундук на карте!
            let field_type = table_config.and_then(|t| t.fields.iter().find(|f| f.name == field_name).map(|f| f.field_type.as_str())).unwrap_or("text"); // Тип клада — что сортируем?            

            joined_rows.sort_by(|a_set, b_set| { // Сортируем флот — порядок в трюме!
                let a_row = a_set.iter().find(|(a, _)| a == alias || (alias.is_empty() && a == &query.alias)); // Ищем добычу по кличке!
                let b_row = b_set.iter().find(|(a, _)| a == alias || (alias.is_empty() && a == &query.alias)); // Ищем вторую добычу!
                let a_val = a_row.and_then(|(_, r)| r.data.get(field_name)); // Хватаем клад первой строки!
                let b_val = b_row.and_then(|(_, r)| r.data.get(field_name)); // Хватаем клад второй строки!
                let cmp = match (a_val, b_val, field_type) { // Сравниваем — по типу всё чётко!
                    (Some(Value::Numeric(a)), Some(Value::Numeric(b)), "numeric") => a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal), // Числа — дублоны в порядке!
                    (Some(Value::Timestamp(a)), Some(Value::Timestamp(b)), "timestamp") => a.cmp(b), // Время — метки на карте!
                    (Some(Value::Boolean(a)), Some(Value::Boolean(b)), "boolean") => a.cmp(b), // Да/Нет — ром или вода?
                    (Some(a), Some(b), _) => a.to_string().cmp(&b.to_string()), // Текст — имена по алфавиту!
                    (Some(a), None, _) => a.to_string().cmp(&String::new()), // Есть у одного — он выше!
                    (None, Some(b), _) => String::new().cmp(&b.to_string()), // Нет у первого — он ниже!
                    (None, None, _) => std::cmp::Ordering::Equal, // Оба пусты — равны!
                };
                if *ascending { cmp } else { cmp.reverse() } // ASC или DESC — порядок наш!
            });
        }

        // Применяем смещение и лимит — грабим с умом!
        let offset = query.offset.unwrap_or(0); // С какого дублона начинаем — по умолчанию с первого!
        let limit = query.limit; // Сколько берём — или всё, если лимита нет!
        let start = offset.min(joined_rows.len()); // Не выходим за борт — обрезаем смещение!
        let end = match limit {
            Some(lim) => (start + lim).min(joined_rows.len()), // Конец — лимит или край трюма!
            None => joined_rows.len(), // Без лимита — до последнего сокровища!
        };
        joined_rows = joined_rows.into_iter().skip(start).take(end - start).collect(); // Пропускаем и берём нужное!

        // Готовим список добычи — что показываем?
        let field_order: Vec<String> = if query.fields == vec!["*".to_string()] {
            config.tables.iter().find(|t| t.name == query.table).map_or(vec![], |t| t.fields.iter().map(|f| f.name.clone()).collect()) // Всё из основного сундука!
        } else {
            query.fields.clone() // Только выбранное — жадность под контролем!
        };

        let mut results: Vec<HashMap<String, String>> = Vec::new(); // Карта добычи — чистый лист!
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new(); // Список виденного — дубли в бан!

        // Формируем добычу — красиво и по полочкам!
        for row_set in joined_rows.iter() {
            let mut result = HashMap::new(); // Новый сундук для строки!
            if query.fields == vec!["*".to_string()] { // Всё? Гребём лопатой!
                let fields = if field_order.is_empty() {
                    let mut keys: Vec<&String> = row_set[0].1.data.keys().collect(); // Все ключи — полный улов!
                    keys.sort(); // Сортируем — порядок в трюме!
                    keys.into_iter().map(|k| k.to_string()).collect()
                } else {
                    field_order.clone() // Берём по списку — точность!
                };
                for field in &fields {
                    if let Some(value) = row_set[0].1.data.get(field) { // Хватаем клад из основного сундука!
                        result.insert(field.clone(), value.to_string()); // Кидаем в результат!
                    }
                }
            } else { // Выборочно? Целимся точно!
                for field in &field_order {
                    let (alias, field_name) = field.split_once('.').unwrap_or(("", field)); // Разделяем кличку и клад!
                    let row = row_set.iter().find(|(a, _)| a == alias || (alias.is_empty() && a == &query.alias)); // Ищем нужный корабль!
                    if let Some((_, r)) = row {
                        if let Some(value) = r.data.get(field_name) { // Хватаем клад!
                            result.insert(field.clone(), value.to_string()); // Кидаем в сундук!
                        }
                    }
                }
            }
            let mut keys: Vec<&String> = result.keys().collect(); // Собираем ключи — проверяем добычу!
            keys.sort(); // Сортируем — порядок в хаосе!
            let row_key: String = keys.iter()
                .map(|k| format!("{}:{}", k, result.get(*k).unwrap())) // Формируем метку строки!
                .collect::<Vec<String>>()
                .join("|"); // Склеиваем — уникальный след!
            if !result.is_empty() && seen.insert(row_key) { // Не пусто и ново? В улов!
                results.push(result);
            }
        }
        
        if results.is_empty() { None } else { Some(results) } // Пусто? None! Есть добыча? Some!
    }

    // Вставляем добычу — новый груз в трюм!
    async fn execute_insert(&self, query: Query) {
        let autoincrement_field = self.get_autoincrement_field(&query.table).await; // Ищем авто-ID — кто считает?
        // Берём или создаём трюм — место для новенького!
        let table_data = self.tables.entry(query.table.clone())
            .or_insert_with(|| Arc::new(DashMap::with_hasher(BuildHasherDefault::<AHasher>::default())))
            .clone();
        let config = self.config.read().await; // Читаем карту — где настройки?
        let table_config = config.tables.iter().find(|t| t.name == query.table); // Находим сундук!

        // Проходим по добыче — грузим всё в трюм!
        for mut query_values in query.values {
            let mut id = if let Some(field) = &autoincrement_field {
                if let Some(value) = query_values.get(field) {
                    value.parse::<i32>().unwrap_or_else(|_| table_data.iter().map(|r| r.id).max().unwrap_or(0) + 1) // Есть значение? Парсим или новый ID!
                } else {
                    table_data.iter().map(|r| r.id).max().unwrap_or(0) + 1 // Нет значения? Считаем сами!
                }
            } else {
                table_data.iter().map(|r| r.id).max().unwrap_or(0) + 1 // Без авто? Новый ID!
            };

            // Проверяем ID — никаких повторов!
            while table_data.contains_key(&id) { id += 1; }

            // Добавляем ID в добычу — метка на месте!
            if let Some(field) = &autoincrement_field {
                query_values.insert(field.clone(), id.to_string());
            }

            let mut typed_data = HashMap::new(); // Новый сундук с типами — порядок в хаосе!
            // Типизируем добычу — золото, ром или карты?
            for (key, value) in query_values {
                let field_type = table_config.and_then(|t| t.fields.iter().find(|f| f.name == key).map(|f| f.field_type.as_str())).unwrap_or("text"); // Тип клада — что за добро?
                let typed_value = match field_type {
                    "numeric" => Value::Numeric(value.parse::<f64>().unwrap_or(0.0)), // Числа — дублоны в счёт!
                    "timestamp" => Value::Timestamp(value.parse::<i64>().unwrap_or(0)), // Время — метка на карте!
                    "boolean" => Value::Boolean(value.parse::<bool>().unwrap_or(false)), // Да/Нет — ром в трюме?
                    _ => Value::Text(value), // Текст — имена и названия!
                };
                typed_data.insert(key, typed_value); // Кидаем в сундук с типами!
            }

            let row = Row { id, data: typed_data }; // Новая добыча — свежий улов!
            table_data.insert(row.id, row.clone()); // Грузим в трюм!
            self.update_indexes(&query.table, &row, false).await; // Обновляем метки — всё под контролем!
        }
        // Не сохраняем сразу на диск — WAL уже зафиксировал изменения!
        self.join_cache.retain(|key, _| !key.contains(&query.table)); // Чистим кэш — старое долой!
    }

    // Обновляем добычу — подкручиваем гайки!
    async fn execute_update(&self, query: Query) {
        // Берём сундук — есть ли что добавить?
        if let Some(table) = self.tables.get(&query.table) {
            // Собираем добычу — полный список!
            let mut to_update = table.iter().map(|r| r.clone()).collect::<Vec<Row>>();
            // Фильтруем, если есть условия — только нужное!
            if !query.where_clauses.is_empty() {
                to_update = self.filter_rows(&query.table, to_update, &query.where_clauses).await;
            }
            // Есть что обновить? Вперёд!
            if let Some(update_values) = query.values.first() {
                let config = self.config.read().await; // Читаем карту — где настройки?
                let table_config = config.tables.iter().find(|t| t.name == query.table); // Находим сундук!
                for mut row in to_update {
                    self.update_indexes(&query.table, &row, true).await; // Убираем старые метки!
                    let mut new_data = row.data.clone(); // Копируем сундук — работаем с запасом!
                    // Типизируем новые ценности — порядок в трюме!
                    for (key, value) in update_values {
                        let field_type = table_config.and_then(|t| t.fields.iter().find(|f| f.name == *key).map(|f| f.field_type.as_str())).unwrap_or("text"); // Тип клада — что меняем?
                        let typed_value = match field_type {
                            "numeric" => Value::Numeric(value.parse::<f64>().unwrap_or(0.0)), // Числа — новый счёт дублонов!
                            "timestamp" => Value::Timestamp(value.parse::<i64>().unwrap_or(0)), // Время — новая метка!
                            "boolean" => Value::Boolean(value.parse::<bool>().unwrap_or(false)), // Да/Нет — ром или вода?
                            _ => Value::Text(value.clone()), // Текст — новое имя!
                        };
                        new_data.insert(key.clone(), typed_value); // Обновляем сундук!
                    }
                    row.data = new_data; // Грузим обновлённый сундук!
                    self.update_indexes(&query.table, &row, false).await; // Новые метки — готово!
                    table.insert(row.id, row); // Обновляем трюм!
                }
                // Не сохраняем сразу на диск — WAL уже зафиксировал изменения!
                self.join_cache.retain(|key, _| !key.contains(&query.table)); // Чистим кэш — без хлама!
            }
        }
    }

    // Удаляем добычу — чистим трюм от лишнего!
    async fn execute_delete(&self, query: Query) {
        // Есть сундук? Убираем ненужное!
        if let Some(table) = self.tables.get(&query.table) {
            // Фильтруем добычу — что под нож?
            let to_delete = self.filter_rows(&query.table, table.iter().map(|r| r.clone()).collect::<Vec<Row>>(), &query.where_clauses).await;
            for row in to_delete {
                self.update_indexes(&query.table, &row, true).await; // Убираем метки — следов не будет!
                table.remove(&row.id); // Выкидываем за борт — чистота!
            }
            // Не сохраняем сразу на диск — WAL уже зафиксировал изменения!
            self.join_cache.retain(|key, _| !key.contains(&query.table)); // Чистим кэш — без остатков!
        }
    }
}
