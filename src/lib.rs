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
use std::time::{SystemTime, UNIX_EPOCH}; // Часы капитана — метки времени для шторма!
use std::error::Error; // Ошибки — штормы на горизонте!
use std::fmt; // Форматируем вести с корабля!

type Hasher = BuildHasherDefault<AHasher>; // Хэшер — наш верный помощник!

// Ошибки — штормы и рифы, что топят корабль!
#[derive(Debug)]
pub enum DbError {
    TableNotFound(String),          // Таблица не найдена — сундук затерялся в море!
    DuplicateValue(String, String), // Нарушение уникальности — два пирата с одним именем!
    InvalidValue(String, String),   // Некорректное значение — ром вместо золота!
    IoError(String),                // Ошибка ввода-вывода — шторм унёс диск!
    SerializationError(String),     // Ошибка сериализации — карта в байтах порвана!
    ConfigError(String),            // Ошибка конфига — карта сокровищ с дыркой!
    Generic(String),                // Общая ошибка — чёртова буря всё смешала!
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbError::TableNotFound(table) => write!(f, "Йо-хо-хо, сундук с именем '{}' на карте не значится!", table),
            DbError::DuplicateValue(field, value) => write!(f, "Кракен заметил дубликат! Поле '{}' уже хранит '{}'.", field, value),
            DbError::InvalidValue(field, value) => write!(f, "Арр! '{}' в поле '{}' — это не добыча, а мусор с палубы!", value, field),
            DbError::IoError(msg) => write!(f, "Шторм потопил сундук! Ошибка на диске: {}", msg),
            DbError::SerializationError(msg) => write!(f, "Проклятье старого пирата! Не могу зашифровать добычу: {}", msg),
            DbError::ConfigError(msg) => write!(f, "Карта сокровищ порвана! Ошибка в конфиге: {}", msg),
            DbError::Generic(msg) => write!(f, "Чёртова буря! Что-то пошло не так: {}", msg),
        }
    }
}

impl Error for DbError {} // Ошибки — часть пиратской жизни!

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
    autoincrement_cache: Arc<DashMap<String, DashMap<String, i64, Hasher>, Hasher>>, // Кэш автоинкрементов — считаем метки для новичков!
}

// Запрос — наш план захвата добычи!
#[derive(Debug, Clone)]
pub struct Query {
    pub table: String,                    // Куда лезем за сокровищами?
    pub fields: Vec<String>,             // Что берём из сундука?
    pub alias: String,                   // Прозвище — чтобы не спутать!
    pub joins: Vec<(String, String, String, String)>, // Связи — собираем флот!
    pub where_clauses: Vec<Vec<Condition>>, // Условия — отсекаем лишних! Внешний Vec — OR, внутренний — AND!
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
        pub fn $method<T: Into<String>>(&mut self, field: &str, value: T) -> &mut Self {
            // Если фильтров нет, создаём пустой список
            if self.where_clauses.is_empty() { self.where_clauses.push(Vec::new()); } // Новый фильтр — чистый лист!
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

// Запрос в базу — теперь с умными цепочками и без потерь!
impl Query {
    // Задаём поля — что хватать из базы
    pub fn fields(&mut self, fields: Vec<&str>) -> &mut Self {
        self.fields = fields.into_iter().map(|s| s.to_string()).collect();
        self // Хватаем добычу и плывём дальше!
        // Цепочка мутирует — теперь с полями!
    }

    // Псевдоним — кличка для сундука!
    pub fn alias(&mut self, alias: &str) -> &mut Self {
        self.alias = alias.to_string();
        self // Даём кличку и вперёд!
        // Новый псевдоним — стильный шильдик!
    }

    // Джоин — связываем таблицы, как конструктор!
    pub fn join(&mut self, table: &str, alias: &str, on_left: &str, on_right: &str) -> &mut Self {
        self.joins.push((table.to_string(), alias.to_string(), on_left.to_string(), on_right.to_string()));
        self // Связываем флот и плывём!
        // Джоин в деле — флот растёт!
    }

    // Значения — кидаем данные в запрос, без лишних рук!
    pub fn values<V>(&mut self, values: V) -> &mut Self where V: IntoValues {
        self.values = values.into_values();
        self // Грузим сундук и дальше!
        // Добыча на борту — грузим без потерь!
    }

    // Условия — фильтры для точных ударов!
    add_condition!(where_eq, Eq);     // Точный удар!
    add_condition!(where_lt, Lt);     // Мелочь в сторону!
    add_condition!(where_gt, Gt);     // Только крупняк!
    add_condition!(where_contains, Contains); // Ищем тайники!

    // Где "в списке" — проверка по шпаргалке!
    pub fn where_in<T: Into<String>>(&mut self, field: &str, values: Vec<T>) -> &mut Self {
        if self.where_clauses.is_empty() { self.where_clauses.push(Vec::new()); } // Пусто? Новый список!
        self.where_clauses.last_mut().unwrap().push(Condition::In(
            field.to_string(),
            values.into_iter().map(Into::into).collect()
        ));
        self // По шпаргалке и дальше!
        // IN добавлен — шпаргалка в кармане!
    }

    // Где "между" — диапазон для умников!
    pub fn where_between<T: Into<String>>(&mut self, field: &str, min: T, max: T) -> &mut Self {
        if self.where_clauses.is_empty() { self.where_clauses.push(Vec::new()); } // Пусто? Новый фильтр!
        self.where_clauses.last_mut().unwrap().push(Condition::Between(
            field.to_string(),
            min.into(),
            max.into()
        ));
        self // Диапазон и вперёд!
        // BETWEEN в игре — диапазон на мушке!
    }

    // Сортировка — порядок в трюме, ASC или DESC!
    pub fn order_by(&mut self, field: &str, ascending: bool) -> &mut Self {
        self.order_by = Some((field.to_string(), ascending)); // Поле и порядок: ASC=true, DESC=false — всё под контролем!
        self // Цепочка — наш герой!
        // Сортировка готова — трюм в строю!
    }

    // Группировка — делим добычу по кучкам!
    pub fn group_by(&mut self, field: &str) -> &mut Self {
        self.group_by = Some(field.to_string()); // Считаем добычу по полям — порядок в хаосе!
        self // Цепочка не рвётся!
        // Группы на месте — кучки считаем!
    }

    // Лимит — сколько добычи утащить с корабля!
    pub fn limit(&mut self, count: usize) -> &mut Self {
        self.limit = Some(count); // Устанавливаем лимит — не больше этого в сундук!
        self // Цепочка — плывём дальше!
        // Лимит на борту — не перегрузимся!
    }

    // Смещение — с какого дублона начинаем грабёж!
    pub fn offset(&mut self, start: usize) -> &mut Self {
        self.offset = Some(start); // Устанавливаем смещение — пропускаем первые сокровища!
        self // Цепочка — на абордаж!
        // Смещение врубили — пропускаем лишнее!
    }

    // Выполняем запрос — время жать на кнопку с проверкой ошибок!
    pub async fn execute(self, db: &Database) -> Result<Option<Vec<HashMap<String, String>>>, DbError> {
        match self.op {
            QueryOp::Select => db.execute_select(self).await, // Читаем добычу с умом!
            QueryOp::Insert => {
                // Записываем операцию в WAL — безопасность прежде всего!
                let operation = WalOperation::Insert {
                    table: self.table.clone(),
                    values: self.values.clone(),
                };
                db.log_to_wal(&operation).await?; // Лог в WAL — не потеряем!
                db.execute_insert(self).await?; // Грузим с проверкой!
                Ok(None) // Ничего не возвращаем — груз в трюме!
            }
            QueryOp::Update => {
                if let Some(values) = self.values.first() {
                    // Записываем операцию в WAL — фиксируем изменения!
                    let operation = WalOperation::Update {
                        table: self.table.clone(),
                        values: values.clone(),
                        where_clauses: self.where_clauses.clone(),
                    };
                    db.log_to_wal(&operation).await?; // WAL в курсе — всё под контролем!
                    db.execute_update(self).await?; // Обновляем с гарантией!
                }
                Ok(None) // Обновили — и вперёд!
            }
            QueryOp::Delete => {
                // Записываем операцию в WAL — убираем с гарантией!
                let operation = WalOperation::Delete {
                    table: self.table.clone(),
                    where_clauses: self.where_clauses.clone(),
                };
                db.log_to_wal(&operation).await?; // WAL записал — чистим смело!
                db.execute_delete(self).await?; // Удаляем с проверкой!
                Ok(None) // Выкинули — чистота!
            }
        }
    }
}

// "Пульт управления" — база в наших руках!
impl Database {
    // Создаём базу — как собрать корабль с нуля!
    pub async fn new(data_dir: &str, config_file: &str) -> Result<Self, DbError> {
        // Проверяем тайник — есть ли берег для сокровищ?
        if !Path::new(data_dir).exists() {
            create_dir_all(data_dir)
                .await
                .map_err(|e| DbError::IoError(format!("Не удалось выкопать яму для сокровищ: {}", e)))?; // Копаем яму, если её нет!
            // Яма готова — тайник на месте!
        }

        // Читаем карту — где спрятан план?
        let config = if !Path::new(config_file).exists() {
            println!("Карта '{}' затерялась в море, берём пустой трюм!", config_file);
            DbConfig::default() // Нет карты? Плывём налегке!
        } else {
            let config_str = tokio::fs::read_to_string(config_file)
                .await
                .map_err(|e| DbError::IoError(format!("Не удалось прочитать карту сокровищ '{}': {}", config_file, e)))?; // Читаем план!
            toml::from_str(&config_str)
                .map_err(|e| DbError::ConfigError(format!("Не удалось расшифровать карту '{}': {}", config_file, e)))? // Парсим или паника!
        };
        let config = Arc::new(RwLock::new(config)); // Прячем под замок — надёжность!

        // Открываем WAL-файл — журнал для операций!
        let wal_path = format!("{}/wal.log", data_dir);
        let wal_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path)
            .await
            .map_err(|e| DbError::IoError(format!("Не удалось открыть журнал WAL '{}': {}", wal_path, e)))?; // WAL готов — безопасность на борту!
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
            autoincrement_cache: Arc::new(DashMap::with_hasher(BuildHasherDefault::<AHasher>::default())), // Кэш для авто-ID — метки для новобранцев!
        };

        // Создаём тайник для данных — копаем яму! (уже проверено выше)
        create_dir_all(data_dir)
            .await
            .map_err(|e| DbError::IoError(format!("Не удалось создать трюм '{}': {}", data_dir, e)))?; // Повторяем на всякий случай — бережёного море бережёт!
        db.load_tables_from_disk().await?; // Загружаем добычу с диска — оживляем корабль!
        db.recover_from_wal().await?; // Восстанавливаем из WAL — если есть несохранённые операции!

        // Клонируем и запускаем шпиона за картой — следим за изменениями!
        let db_clone = db.clone();
        tokio::spawn(async move { db_clone.watch_config().await });

        // Запускаем фоновую задачу для сброса WAL в основной файл каждые 60 секунд!
        let db_flush = db.clone();
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                if let Err(e) = db_flush.flush_wal_to_bin().await {
                    println!("Ошибка при сбросе WAL: {}", e); // Логируем шторм, но держим курс!
                }
                // Каждые 60 сек — WAL в .bin, порядок на корабле!
            }
        });

        Ok(db) // Готово — корабль на плаву!
    }

    // Запускаторы запросов — штурвал в руках!
    query_builder!(select, Select); // Читаем добычу!
    query_builder!(insert, Insert); // Грузим в трюм!
    query_builder!(update, Update); // Меняем ром на золото!
    query_builder!(delete, Delete); // Выкидываем за борт!

    // Записываем операцию в WAL — фиксируем намерения с проверкой!
    async fn log_to_wal(&self, operation: &WalOperation) -> Result<(), DbError> {
        let encoded = bincode::serialize(operation)
            .map_err(|e| DbError::SerializationError(format!("Не удалось закодировать операцию: {}", e)))?; // Кодируем операцию — в байты!
        {
            let mut wal = self.wal_file.lock().await; // Захватываем журнал!
            wal.write_all(&encoded)
                .await
                .map_err(|e| DbError::IoError(format!("Не удалось записать в WAL: {}", e)))?; // Пишем в WAL — шустро!
            wal.flush()
                .await
                .map_err(|e| DbError::IoError(format!("Не удалось сбросить WAL на диск: {}", e)))?; // Убеждаемся, что всё на диске!
        }
        Ok(()) // Всё записано — полный вперёд!
        // WAL в деле — данные под замком!
    }

    // Восстанавливаем из WAL — спасаем добычу после шторма!
    async fn recover_from_wal(&self) -> Result<(), DbError> {
        let wal_path = format!("{}/wal.log", self.data_dir);
        if !Path::new(&wal_path).exists() {
            return Ok(()); // Нет WAL? Нечего восстанавливать!
        }
        let file = File::open(&wal_path)
            .await
            .map_err(|e| DbError::IoError(format!("Не удалось открыть журнал WAL '{}': {}", wal_path, e)))?; // Открываем журнал!
        let mut reader = BufReader::new(file);
        let mut buffer = Vec::new(); // Буфер для чтения!
        reader.read_to_end(&mut buffer)
            .await
            .map_err(|e| DbError::IoError(format!("Не удалось прочитать журнал WAL '{}': {}", wal_path, e)))?; // Читаем весь WAL!
        if buffer.is_empty() {
            return Ok(()); // Пустой WAL? Выходим!
        }
        let operations: Vec<WalOperation> = bincode::deserialize(&buffer)
            .map_err(|e| DbError::SerializationError(format!("Не удалось раскодировать журнал WAL: {}", e)))?; // Читаем операции!
        for op in operations {
            match op {
                WalOperation::Insert { table, values } => {
                    let query = Query {
                        table,
                        op: QueryOp::Insert,
                        values,
                        ..Default::default()
                    };
                    self.execute_insert(query).await?; // Применяем вставку — без паники при ошибке!
                }
                WalOperation::Update { table, values, where_clauses } => {
                    let query = Query {
                        table,
                        op: QueryOp::Update,
                        values: vec![values],
                        where_clauses,
                        ..Default::default()
                    };
                    self.execute_update(query).await?; // Применяем обновление!
                }
                WalOperation::Delete { table, where_clauses } => {
                    let query = Query {
                        table,
                        op: QueryOp::Delete,
                        where_clauses,
                        ..Default::default()
                    };
                    self.execute_delete(query).await?; // Применяем удаление!
                }
            }
        }
        // Очищаем WAL после восстановления!
        File::create(&wal_path)
            .await
            .map_err(|e| DbError::IoError(format!("Не удалось очистить журнал WAL '{}': {}", wal_path, e)))?; // Ошибка? Пропускаем — журнал чист!
        Ok(()) // WAL восстановлен — корабль на плаву!
    }

    // Сбрасываем WAL в основной файл — сохраняем порядок!
    async fn flush_wal_to_bin(&self) -> Result<(), DbError> {
        // Сохраняем все таблицы в .bin!
        for table_name in self.tables.iter().map(|t| t.key().clone()).collect::<Vec<_>>() {
            self.save_table(&table_name).await?; // Спасаем каждый трюм!
        }
        // Очищаем WAL — всё синхронизировано!
        let wal_path = format!("{}/wal.log", self.data_dir);
        File::create(&wal_path)
            .await
            .map_err(|e| DbError::IoError(format!("Не удалось очистить WAL '{}': {}", wal_path, e)))?; // Ошибка? Пропускаем — корабль плывёт!
        Ok(()) // WAL сброшен — диск в курсе!
    }

    // Ищем уникальное поле — кто тут особый?
    async fn get_unique_fields(&self, table_name: &str) -> Vec<String> {
        self.config.read().await.tables.iter()
            .find(|t| t.name == table_name) // Находим сундук!
            .map(|t| t.fields.iter().filter(|f| f.unique.unwrap_or(false)).map(|f| f.name.clone()).collect()) // Выцепляем уникальный клад!
            .unwrap_or_default() // Нет? Пустой список — плывём дальше!
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
    async fn load_tables_from_disk(&self) -> Result<(), DbError> {
        let mut entries = tokio::fs::read_dir(&self.data_dir)
            .await
            .map_err(|e| DbError::IoError(format!("Не удалось открыть трюм '{}': {}", self.data_dir, e)))?; // Читаем тайник — где наш склад?

        // Проходим по сундукам — что тут у нас?
        while let Some(entry) = entries.next_entry().await
            .map_err(|e| DbError::IoError(format!("Ошибка при чтении трюма: {}", e)))?
        {
            // Берём только .bin — остальное не трогаем!
            if entry.path().extension() == Some("bin".as_ref()) {
                // Имя сундука — выдираем из файла!
                let table_name = entry.path().file_stem().unwrap().to_str().unwrap().to_string();
                // Открываем сундук — лезем в закрома!
                let mut file = File::open(&entry.path())
                    .await
                    .map_err(|e| DbError::IoError(format!("Не удалось открыть сундук '{}': {}", entry.path().display(), e)))?;
                let mut buffer = Vec::new(); // Буфер — наш временный ящик!
                file.read_to_end(&mut buffer)
                    .await
                    .map_err(|e| DbError::IoError(format!("Не удалось прочитать добычу из '{}': {}", table_name, e)))?;
                if buffer.is_empty() {
                    continue; // Пусто? Далее!
                }
                // Распаковываем добычу — сокровища в руках!
                let rows: HashMap<i32, Row> = bincode::deserialize(&buffer)
                    .map_err(|e| DbError::SerializationError(format!("Не удалось раскодировать добычу '{}': {}", table_name, e)))?;

                // Новый трюм — свежий контейнер!
                let table = Arc::new(DashMap::with_hasher(BuildHasherDefault::<AHasher>::default()));
                let config = self.config.read().await;
                let table_config = config.tables.iter().find(|t| t.name == table_name);

                // Собираем автоинкременты — кто считает сам?
                let autoincrement_fields: Vec<String> = table_config
                    .map(|t| t.fields.iter().filter(|f| f.autoincrement.unwrap_or(false)).map(|f| f.name.clone()).collect())
                    .unwrap_or_default();

                let autoincrement_map = DashMap::with_hasher(BuildHasherDefault::<AHasher>::default());
                self.autoincrement_cache.insert(table_name.clone(), autoincrement_map.clone());

                // Проходим по добыче — грузим добро и считаем метки!
                for (id, row) in rows {
                    table.insert(id, row.clone());
                    for field in &autoincrement_fields {
                        if let Some(Value::Numeric(val)) = row.data.get(field) {
                            let mut current_max = autoincrement_map.entry(field.clone()).or_insert(0);
                            *current_max = (*current_max).max(*val as i64); // Обновляем максимум — новый рекорд!
                        }
                    }
                }
                // Сохраняем трюм — место занято!
                self.tables.insert(table_name.clone(), table);
                // Перестраиваем метки — ускоряем поиск!
                self.rebuild_indexes(&table_name).await;
            }
        }
        Ok(()) // Добыча на борту — корабль жив!
    }

    // Сохраняем трюм — прячем добычу на диск!
    async fn save_table(&self, table_name: &str) -> Result<(), DbError> {
        // Берём трюм — есть ли что спасать?
        if let Some(table) = self.tables.get(table_name) {
            let path = format!("{}/{}.bin", self.data_dir, table_name); // Путь для сундука — наш цифровой сейф!
            let rows: HashMap<i32, Row> = table.iter().map(|r| (*r.key(), r.value().clone())).collect(); // Собираем добычу — всё в кучу!
            let encoded = bincode::serialize(&rows)
                .map_err(|e| DbError::SerializationError(format!("Не удалось закодировать таблицу '{}': {}", table_name, e)))?; // Кодируем — превращаем в байты!
            File::create(&path)
                .await
                .map_err(|e| DbError::IoError(format!("Не удалось создать файл '{}': {}", path, e)))?
                .write_all(&encoded)
                .await
                .map_err(|e| DbError::IoError(format!("Не удалось записать таблицу '{}': {}", table_name, e)))?; // Пишем на диск — теперь не пропадёт!
            // Диск в курсе — добыча в сейфе!
        }
        Ok(()) // Трюм сохранён — полный вперёд!
    }

    // Перестраиваем метки — ускоряем корабль до турбо-режима!
    async fn rebuild_indexes(&self, table_name: &str) {
        if let Some(table) = self.tables.get(table_name) {
            let config = self.config.read().await;
            if let Some(table_config) = config.tables.iter().find(|t| t.name == table_name) {
                for field in &table_config.fields {
                    let indexed = field.indexed.unwrap_or(false);
                    let fulltext = field.fulltext.unwrap_or(false);
                    if indexed {
                        let index = DashMap::with_hasher(BuildHasherDefault::<AHasher>::default());
                        for row in table.iter() {
                            if let Some(value) = row.data.get(&field.name) {
                                index.entry(value.to_string()).or_insert_with(Vec::new).push(row.id);
                            }
                        }
                        self.indexes.entry(table_name.to_string())
                            .or_insert_with(|| Arc::new(DashMap::with_hasher(BuildHasherDefault::<AHasher>::default())))
                            .insert(field.name.clone(), Arc::new(index));
                    }
                    if fulltext {
                        let ft_index = DashMap::with_hasher(BuildHasherDefault::<AHasher>::default());
                        for row in table.iter() {
                            if let Some(Value::Text(text)) = row.data.get(&field.name) {
                                ft_index.entry(text.to_lowercase()).or_insert_with(Vec::new).push(row.id);
                            }
                        }
                        self.fulltext_indexes.entry(table_name.to_string())
                            .or_insert_with(|| Arc::new(DashMap::with_hasher(BuildHasherDefault::<AHasher>::default())))
                            .insert(field.name.clone(), Arc::new(ft_index));
                    }
                }
            }
        }
        // Индексы в строю — поиск на турбо!
    }

    // Применяем карту — корабль в курсе всех новостей!
    async fn apply_config(&self) {
        let config = self.config.read().await; // Читаем настройки — что у нас в плане?
        // Проходим по сундукам — все на месте?
        for table_config in &config.tables {
            // Нет трюма? Создаём — без паники!
            if !self.tables.contains_key(&table_config.name) {
                self.tables.insert(table_config.name.clone(), Arc::new(DashMap::with_hasher(BuildHasherDefault::<AHasher>::default())));
                let _ = self.save_table(&table_config.name).await; // Сохраняем — на диск, чтобы не забыть!
            }
            self.rebuild_indexes(&table_config.name).await; // Обновляем метки — скорость наше всё!

            // Обновляем кэш автоинкрементов — считаем новобранцев!
            let autoincrement_fields: Vec<String> = table_config.fields.iter()
                .filter(|f| f.autoincrement.unwrap_or(false))
                .map(|f| f.name.clone())
                .collect();
            let autoincrement_map = self.autoincrement_cache
                .entry(table_config.name.clone())
                .or_insert_with(|| DashMap::with_hasher(BuildHasherDefault::<AHasher>::default()))
                .clone();

            if let Some(table) = self.tables.get(&table_config.name) {
                for row in table.iter() {
                    for field in &autoincrement_fields {
                        if let Some(Value::Numeric(val)) = row.data.get(field) {
                            let mut current_max = autoincrement_map.entry(field.clone()).or_insert(0);
                            *current_max = (*current_max).max(*val as i64); // Новый максимум — метка на месте!
                        }
                    }
                }
            }
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

    // Фильтруем добычу — выцепляем нужное с умом и без лишних клонов!
    async fn filter_rows(&self, table_name: &str, rows: &[Row], where_clauses: &[Vec<Condition>]) -> Vec<Row> {
        let mut filtered = rows.to_vec(); // Исходный набор строк
        for and_group in where_clauses {
            let mut group_result = Vec::new();
            for condition in and_group {
                let filtered_subset = match condition {
                    Condition::Eq(field, value) => {
                        self.index_filter(table_name, field, value, &filtered, |v, val| v.to_string() == val)
                    }
                    Condition::Contains(field, value) => {
                        self.fulltext_filter(table_name, field, value, &filtered)
                    }
                    Condition::Lt(field, value) => {
                        filtered.iter().filter(|row| {
                            if let Some(Value::Numeric(n)) = row.data.get(field) {
                                if let Ok(val) = value.parse::<f64>() {
                                    return *n < val;
                                }
                            }
                            false
                        }).cloned().collect()
                    }
                    Condition::Gt(field, value) => {
                        filtered.iter().filter(|row| {
                            if let Some(Value::Numeric(n)) = row.data.get(field) {
                                if let Ok(val) = value.parse::<f64>() {
                                    return *n > val;
                                }
                            }
                            false
                        }).cloned().collect()
                    }
                    Condition::In(field, values) => {
                        let mut result = Vec::new();
                        for value in values {
                            result.extend(self.index_filter(table_name, field, value, &filtered, |v, val| v.to_string() == val));
                        }
                        result
                    }
                    Condition::Between(field, min, max) => {
                        filtered.iter().filter(|row| {
                            if let Some(Value::Numeric(n)) = row.data.get(field) {
                                if let (Ok(min_num), Ok(max_num)) = (min.parse::<f64>(), max.parse::<f64>()) {
                                    return *n >= min_num && *n <= max_num;
                                }
                            }
                            false
                        }).cloned().collect()
                    }
                };
                if group_result.is_empty() {
                    group_result = filtered_subset;
                } else {
                    group_result.retain(|row| filtered_subset.iter().any(|r| r.id == row.id)); // Пересечение (AND)
                }
            }
            filtered = group_result; // Обновляем результат для следующей группы OR
        }
        filtered // Фильтр готов — добыча отсеяна!
        // OR и AND в гармонии — фильтр на высоте!
    }

    // Фильтр по меткам — скорость наше оружие!
    fn index_filter<F>(&self, table_name: &str, field: &str, value: &str, rows: &[Row], pred: F) -> Vec<Row>
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
        rows.iter().filter(|r| r.data.get(field).map_or(false, |v| pred(v, value))).map(|r| r.clone()).collect()
    }

    // Полнотекстовый фильтр — слова под микроскопом!
    fn fulltext_filter(&self, table_name: &str, field: &str, value: &str, rows: &[Row]) -> Vec<Row> {
        let value_lower = value.to_lowercase();
        if let Some(ft_index_map) = self.fulltext_indexes.get(table_name) {
            if let Some(ft_index) = ft_index_map.get(field) {
                let mut ids = Vec::new();
                for entry in ft_index.iter() {
                    if entry.key().contains(&value_lower) {
                        ids.extend(entry.value().clone());
                    }
                }
                ids.sort_unstable();
                ids.dedup();
                return ids.into_iter()
                    .filter_map(|id| self.tables.get(table_name).and_then(|t| t.get(&id).map(|r| r.clone())))
                    .collect();
            }
        }
        // Запасной вариант: фильтрация вручную, если индекса нет
        rows.iter()
            .filter(|r| r.data.get(field).map_or(false, |v| v.to_string().to_lowercase().contains(&value_lower)))
            .cloned()
            .collect()
        // Полнотекст врубили — слова под лупой!
    }

    // Выполняем SELECT — добываем сокровища с проверкой!
    async fn execute_select(&self, query: Query) -> Result<Option<Vec<HashMap<String, String>>>, DbError> {
        let table = self.tables.get(&query.table)
            .ok_or_else(|| DbError::TableNotFound(query.table.clone()))?; // Берём сундук — где добыча?
        // Собираем добычу с кличками — готовим базу!
        let rows: Vec<(String, Row)> = table.iter().map(|r| (query.alias.clone(), r.clone())).collect();
        // Начинаем с простого — каждая добыча в своём наборе!
        let mut joined_rows: Vec<Vec<(String, Row)>> = rows.into_iter().map(|r| vec![r]).collect();

        // Джойним флот — связываем всё как профи!
        for (join_table, join_alias, on_left, on_right) in &query.joins {
            let join_table_data = self.tables.get(join_table)
                .ok_or_else(|| DbError::TableNotFound(join_table.clone()))?; // Берём союзный корабль!
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

        // Фильтруем добычу основной таблицы — отсекаем лишнее с умом!
        let filtered_rows = if !query.where_clauses.is_empty() {
            self.filter_rows(&query.table, &joined_rows.iter().map(|r| r[0].1.clone()).collect::<Vec<Row>>(), &query.where_clauses).await
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
            let table_config = config.tables.iter().find(|t| t.name == *table_name)
                .ok_or_else(|| DbError::TableNotFound(table_name.to_string()))?; // Находим сундук на карте!
            let field_type = table_config.fields.iter().find(|f| f.name == field_name)
                .map(|f| f.field_type.as_str())
                .unwrap_or("text"); // Тип клада — что сортируем?

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

        Ok(if results.is_empty() { None } else { Some(results) }) // Пусто? None! Есть добыча? Some!
        // SELECT сработал — добыча в кармане!
    }

    // Вставляем добычу — новый груз в трюм с проверкой!
    async fn execute_insert(&self, query: Query) -> Result<(), DbError> {
        // Берём или создаём трюм — место для новенького!
        let table_data = self.tables.entry(query.table.clone())
            .or_insert_with(|| Arc::new(DashMap::with_hasher(BuildHasherDefault::<AHasher>::default())))
            .clone();
        let config = self.config.read().await; // Читаем карту — где настройки?
        let table_config = config.tables.iter().find(|t| t.name == query.table)
            .ok_or_else(|| DbError::TableNotFound(query.table.clone()))?; // Находим сундук!

        // Собираем автоинкременты и уникальные поля — кто считает и кто особый?
        let autoincrement_fields: Vec<String> = table_config
            .fields.iter()
            .filter(|f| f.autoincrement.unwrap_or(false))
            .map(|f| f.name.clone())
            .collect();
        let unique_fields = self.get_unique_fields(&query.table).await;
        let autoincrement_map = self.autoincrement_cache
            .entry(query.table.clone())
            .or_insert_with(|| DashMap::with_hasher(BuildHasherDefault::<AHasher>::default()))
            .clone();

        // Считаем максимумы для автоинкрементов — кто тут главный?
        for field in &autoincrement_fields {
            let max_value = table_data.iter()
                .filter_map(|r| r.data.get(field).and_then(|v| if let Value::Numeric(n) = v { Some(*n as i64) } else { None }))
                .max()
                .unwrap_or(0);
            let mut current_max = autoincrement_map.entry(field.clone()).or_insert(0);
            if *current_max < max_value {
                *current_max = max_value; // Новый рекорд — метка на месте!
            }
        }

        // Проходим по добыче — грузим всё в трюм!
        for query_values in query.values {
            let mut typed_data = HashMap::new(); // Новый сундук с типами — порядок в хаосе!
            let row_id = table_data.iter().map(|r| r.id).max().unwrap_or(0) + 1; // Новый ID — место для новичка!

            // Типизируем добычу — золото, ром или карты?
            for (key, value) in &query_values {
                let field_config = table_config.fields.iter().find(|f| f.name == *key)
                    .ok_or_else(|| DbError::InvalidValue(key.clone(), "поле не найдено".to_string()))?; // Поле есть? Или паника!
                let field_type = field_config.field_type.as_str();
                let typed_value = match field_type {
                    "numeric" => Value::Numeric(value.parse::<f64>()
                        .map_err(|_| DbError::InvalidValue(key.clone(), value.clone()))?), // Числа — дублоны в счёт!
                    "timestamp" => Value::Timestamp(value.parse::<i64>()
                        .map_err(|_| DbError::InvalidValue(key.clone(), value.clone()))?), // Время — метка на карте!
                    "boolean" => Value::Boolean(value.parse::<bool>()
                        .map_err(|_| DbError::InvalidValue(key.clone(), value.clone()))?), // Да/Нет — ром в трюме?
                    _ => Value::Text(value.clone()), // Текст — имена и названия!
                };
                typed_data.insert(key.clone(), typed_value); // Кидаем в сундук с типами!
            }

            // Проверяем уникальность — никаких дублей!
            for field in &unique_fields {
                if let Some(value) = query_values.get(field) {
                    let typed_value = typed_data.get(field).unwrap();
                    if table_data.iter().any(|r| r.data.get(field) == Some(typed_value)) {
                        return Err(DbError::DuplicateValue(field.clone(), value.clone())); // Кракен заметил дубликат!
                    }
                }
            }

            // Добавляем автоинкременты — считаем сами, если надо!
            for field_config in &table_config.fields {
                if field_config.autoincrement.unwrap_or(false) && !query_values.contains_key(&field_config.name) {
                    let field_type = field_config.field_type.as_str();
                    let value = match field_type {
                        "numeric" => {
                            let mut current_max = autoincrement_map.entry(field_config.name.clone()).or_insert(0);
                            *current_max += 1; // Новый номер в команде!
                            Value::Numeric(*current_max as f64)
                        }
                        "timestamp" => {
                            let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64; // Метка времени — сейчас!
                            Value::Timestamp(timestamp)
                        }
                        "boolean" => Value::Boolean(true), // По умолчанию — ром есть!
                        "text" => Value::Text(format!("default_{}", row_id)), // Текст по умолчанию — метка новичка!
                        _ => Value::Text("default".to_string()), // Просто запасной вариант!
                    };
                    typed_data.insert(field_config.name.clone(), value); // Кидаем в сундук!
                }
            }

            // Ещё раз проверяем уникальность — после автоинкрементов!
            for field in &unique_fields {
                if let Some(value) = typed_data.get(field) {
                    if table_data.iter().any(|r| r.data.get(field) == Some(value)) {
                        return Err(DbError::DuplicateValue(field.clone(), value.to_string())); // Дубликат? За борт!
                    }
                }
            }

            let row = Row { id: row_id, data: typed_data }; // Новая добыча — свежий улов!
            table_data.insert(row.id, row.clone()); // Грузим в трюм!
            self.update_indexes(&query.table, &row, false).await; // Обновляем метки — всё под контролем!
        }
        // Не сохраняем сразу на диск — WAL уже зафиксировал изменения!
        self.join_cache.retain(|key, _| !key.contains(&query.table)); // Чистим кэш — старое долой!
        Ok(()) // Груз в трюме — полный вперёд!
        // INSERT сработал — трюм пополнен!
    }

    // Обновляем добычу — подкручиваем гайки с проверкой!
    async fn execute_update(&self, query: Query) -> Result<(), DbError> {
        // Берём сундук — есть ли что добавить?
        if let Some(table) = self.tables.get(&query.table) {
            // Собираем добычу — полный список!
            let rows: Vec<Row> = table.iter().map(|r| r.clone()).collect();
            // Фильтруем, если есть условия — только нужное!
            let to_update = if !query.where_clauses.is_empty() {
                self.filter_rows(&query.table, &rows, &query.where_clauses).await
            } else {
                rows
            };
            // Есть что обновить? Вперёд!
            if let Some(update_values) = query.values.first() {
                let config = self.config.read().await; // Читаем карту — где настройки?
                let table_config = config.tables.iter().find(|t| t.name == query.table)
                    .ok_or_else(|| DbError::TableNotFound(query.table.clone()))?; // Находим сундук!
                let unique_fields = self.get_unique_fields(&query.table).await; // Кто тут особый?

                for mut row in to_update {
                    let mut new_data = row.data.clone(); // Копируем сундук — работаем с запасом!
                    // Типизируем новые ценности — порядок в трюме!
                    for (key, value) in update_values {
                        let field_config = table_config.fields.iter().find(|f| f.name == *key)
                            .ok_or_else(|| DbError::InvalidValue(key.clone(), "поле не найдено".to_string()))?; // Поле есть? Или паника!
                        let field_type = field_config.field_type.as_str();
                        let typed_value = match field_type {
                            "numeric" => Value::Numeric(value.parse::<f64>()
                                .map_err(|_| DbError::InvalidValue(key.clone(), value.clone()))?), // Числа — новый счёт дублонов!
                            "timestamp" => Value::Timestamp(value.parse::<i64>()
                                .map_err(|_| DbError::InvalidValue(key.clone(), value.clone()))?), // Время — новая метка!
                            "boolean" => Value::Boolean(value.parse::<bool>()
                                .map_err(|_| DbError::InvalidValue(key.clone(), value.clone()))?), // Да/Нет — ром или вода?
                            _ => Value::Text(value.clone()), // Текст — новое имя!
                        };
                        new_data.insert(key.clone(), typed_value.clone()); // Обновляем сундук!

                        // Проверяем уникальность — никаких дублей!
                        if unique_fields.contains(key) {
                            if table.iter().any(|r| r.id != row.id && r.data.get(key) == Some(&typed_value)) {
                                return Err(DbError::DuplicateValue(key.clone(), value.clone())); // Кракен заметил дубликат!
                            }
                        }
                    }
                    self.update_indexes(&query.table, &row, true).await; // Убираем старые метки!
                    row.data = new_data; // Грузим обновлённый сундук!
                    self.update_indexes(&query.table, &row, false).await; // Новые метки — готово!
                    table.insert(row.id, row); // Обновляем трюм!
                }
                // Не сохраняем сразу на диск — WAL уже зафиксировал изменения!
                self.join_cache.retain(|key, _| !key.contains(&query.table)); // Чистим кэш — без хлама!
            }
        }
        Ok(()) // Обновили — корабль в строю!
        // UPDATE прошёл — трюм в порядке!
    }

    // Удаляем добычу — чистим трюм от лишнего с проверкой!
    async fn execute_delete(&self, query: Query) -> Result<(), DbError> {
        // Есть сундук? Убираем ненужное!
        if let Some(table) = self.tables.get(&query.table) {
            // Собираем добычу — полный список!
            let rows: Vec<Row> = table.iter().map(|r| r.clone()).collect();
            // Фильтруем добычу — что под нож?
            let to_delete = self.filter_rows(&query.table, &rows, &query.where_clauses).await;
            for row in to_delete {
                self.update_indexes(&query.table, &row, true).await; // Убираем метки — следов не будет!
                table.remove(&row.id); // Выкидываем за борт — чистота!
            }
            // Не сохраняем сразу на диск — WAL уже зафиксировал изменения!
            self.join_cache.retain(|key, _| !key.contains(&query.table)); // Чистим кэш — без остатков!
        }
        Ok(()) // Чисто — полный вперёд!
        // DELETE сработал — трюм чист!
    }
}
