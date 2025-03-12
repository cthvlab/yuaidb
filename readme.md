# YUAIDB: Your Uberfast Async In-Memory DataBase 🚀✨

Добро пожаловать в **YUAIDB** — базу данных, которая живёт в оперативке, выполняет SQL Like запросы как Postgres, уделывает Redis в скорости и сохраняет ваши данные на диск быстрее, чем вы скажете "Где мой заказ?". Написана на Rust, потому что мы любим быстрые, безопасные и простые штуки. 😎

## Что это такое? 🤔

YUAIDB — это in-memory база данных для **актуальных данных**, где важна молниеносная скорость чтения и записи. Мы созданы для "горячих" штук вроде пользовательских сессий, заказов в работе или любых данных, которые нужно обновлять и получать на лету. 

- **Скорость**: O(1) операции в памяти, плюс параллелизм через `tokio`.
- **Надёжность**: Каждое изменение сразу на диск — никаких потерь при сбоях.
- **Гибкость**: JOIN, WHERE, ORDER BY, GROUP BY и полнотекстовый поиск!
- **Режимы**: Встраиваемая библиотека или gRPC-сервис — выбирайте свой стиль.

## Зачем это нужно? 🎉

- Хотите базу для актуальных данных быстрее Redis? YUAIDB в деле.
- Нужна железная надёжность для сессий или заказов в работе? Мы пишем на диск после каждого чиха.
- У вас не так много данных? Это Postgres в оперативке!
- Любите Rust и хотите базу с характером? Вы по адресу!

**Важно**: YUAIDB — это не про терабайты и кластеры, для этого лучше использовать другие базы с масштабированием и репликацией или добавьте в YUAIDB это самостоятельно

## Как это работает? 🛠️

YUAIDB держит данные в памяти с использованием `DashMap` для максимальной скорости и многопоточности. 
После каждой операции (`insert`, `update`, `delete`) всё синхронно пишется в `.bin` файлы на диск — никаких "ой, забыл сохранить". 
Перезапуск? Всё грузится обратно в RAM, как по волшебству.


### Ключевые фичи:
- **O(1) в памяти**: Чистая скорость операций без записи на диск.
- **Сохранение на диск**: Синхронно, чтобы ни один заказ или сессия не потерялись.
- **Индексы**: Обычные и полнотекстовые — ищите как профи.
- **JOIN-ы**: Параллельные, с кэшем — для сложных запросов.
- **gRPC**: Сервисный режим для тех, кто хочет управлять через крутой протокол.

## Установка и запуск 🎬

1. **Клонируйте репозиторий**:
   ```bash
   git clone https://github.com/cthvlab/yuaidb.git
   cd yuaidb
   ```

2. **Запустите как встраиваемую базу**:
   ```bash
   cargo run
   ```

3. **Пример конфига** (`dbconfig.toml`):
   ```toml
   [[tables]]
   name = "orders"
   [[tables.fields]]
   name = "order_id"
   type = "string"
   indexed = true
   ```

## Пример использования 💻

### Встраиваемый режим
```rust
use yuaidb::{Database, Row, Condition};
use std::collections::HashMap;

#[tokio::main]
async fn main() {
    let db = Database::new("data", "dbconfig.toml").await;
    db.insert("orders", Row {
        id: 1,
        data: HashMap::from([("order_id".to_string(), "ORD123".to_string())]),
    }).await;
    let results = db.select("orders", vec![Condition::Eq("order_id".to_string(), "ORD123".to_string())], None, None).await;
    println!("Found: {:?}", results);
}
```

### gRPC-сервис
```bash
grpcurl -plaintext -H "login: admin" -H "password: password123" \
  -d '{"table": "orders", "row": {"id": 1, "data": {"order_id": "ORD123"}}}' \
  127.0.0.1:8080 database.DatabaseService/Insert
```

## Сравнение с Redis 😏

| Фича             | YUAIDB (без диска) | YUAIDB (с диском) | Redis (без AOF) | Redis (AOF every write) |
|------------------|--------------------|-------------------|-----------------|-------------------------|
| **Скорость записи** | ~1.2M оп/сек       | ~666k оп/сек      | ~1M оп/сек      | ~50k–100k оп/сек        |
| **Скорость чтения** | ~1.1M оп/сек       | ~1.1M оп/сек      | ~1M оп/сек      | ~1M оп/сек              |
| **JOIN и WHERE**    | Есть!              | Есть!             | Нет             | Нет                     |
| **Сохранение**      | Нет                | Каждую операцию   | Нет             | Каждую операцию (AOF)   |
| **Надёжность**     | Потеря при сбое    | 100% сохранность  | Потеря при сбое | 100% сохранность       |

### Пояснения:
- **YUAIDB (без диска)**: Чистая O(1) скорость в памяти — мы обгоняем Redis за счёт параллелизма и встраиваемости.
- **YUAIDB (с диском)**: Синхронная запись на диск замедляет, но гарантирует сохранность актуальных данных (сессий, заказов).
- **Redis (без AOF)**: Максимальная скорость, но данные теряются при сбое, пока не настроен RDB (снимки) или AOF (лог).
- **Redis (AOF every write)**: Полная надёжность с `fsync` на каждый запрос, но скорость падает в разы.

**Вывод**: Для актуальных данных YUAIDB в памяти быстрее Redis, а с диском — идеальный баланс скорости и надёжности. Redis с AOF "every write" сильно проигрывает по производительности.

## Лицензия 📜

MIT — берите, используйте, веселитесь!

YUAIDB — это ваш билет в мир скорости, надёжности и Rust-магии для актуальных данных. Попробуйте, и пусть ваши заказы и сессии летают как ракета! 🚀


## Авторы

Разработано сообществом ЮАИ [yuai.ru](https://yuai.ru) 

