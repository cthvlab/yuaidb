# YUAIDB: Your Uberfast Async In-memory DataBase 🚀✨

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
   name = "pirates"
   [[tables.fields]]
   name = "id"
   indexed = true
   autoincrement = true
   unique = true
   [[tables.fields]]
   name = "name"
   [[tables.fields]]
   name = "ship_id"
   
   [[tables]]
   name = "ships"
   [[tables.fields]]
   name = "ship_id"
   indexed = true
   autoincrement = true
   unique = true
   [[tables.fields]]
   name = "name"
   [[tables.fields]]
   name = "speed"
   
   ```

## Пример использования 💻

### Встраиваемый режим
```rust
use yuaidb::Database;

#[tokio::main]
async fn main() {
    let db = Database::new("./data", "./config.toml").await;

    db.insert("pirates")
        .values(vec![("pirate_id", "1"), ("name", "Капитан Джек Воробот"), ("ship_id", "101")])
        .execute(&db)
        .await;

    db.insert("pirates")
        .values(vec![("pirate_id", "2"), ("name", "Лихой Билл"), ("ship_id", "102")])
        .execute(&db)
        .await;

    db.insert("ships")
        .values(vec![("ship_id", "101"), ("name", "Чёрная Комета"), ("speed", "0.9c")])
        .execute(&db)
        .await;

    db.insert("ships")
        .values(vec![("ship_id", "102"), ("name", "Астероидный Шторм"), ("speed", "0.7c")])
        .execute(&db)
        .await;

    println!("Кто на чём летает:");
    if let Some(rows) = db.select("pirates")
        .alias("p")
        .fields(vec!["p.name", "s.name", "s.speed"])
        .join("ships", "s", "s.ship_id", "p.ship_id")
        .execute(&db)
        .await
    {
        for row in rows {
            println!(
                "Пират {} управляет кораблём {} со скоростью {}",
                row.get("p.name").unwrap(),
                row.get("s.name").unwrap(),
                row.get("s.speed").unwrap()
            );
        }
    }

    // Обновляем корабль для Джека
    db.update("pirates")
        .values(vec![("ship_id", "102")]) // Меняем корабль на "Астероидный Шторм"
        .where_eq("pirate_id", "1")
        .execute(&db)
        .await;

    println!("\nПосле смены корабля:");
    if let Some(rows) = db.select("pirates")
        .alias("p")
        .fields(vec!["p.name", "s.name"])
        .join("ships", "s", "s.ship_id", "p.ship_id")
        .where_eq("p.pirate_id", "1")
        .execute(&db)
        .await
    {
        for row in rows {
            println!("Теперь {} летает на {}", row.get("p.name").unwrap(), row.get("s.name").unwrap());
        }
    }
}
```
#### SQL-подобный синтаксис:
```SQL
.select("pirates").alias("p").fields(vec!["p.name", "s.name"]).join("ships", "s", "s.ship_id", "p.ship_id") 
SELECT p.name, s.name FROM pirates p JOIN ships s ON s.ship_id = p.ship_id.
```


### gRPC-сервис
```bash
grpcurl \
  -plaintext \
  -d '{
    "table": "pirates",
    "fields": ["p.name", "s.name"],
    "alias": "p",
    "joins": [
      {
        "table": "ships",
        "alias": "s",
        "on_left": "s.ship_id",
        "on_right": "p.ship_id"
      }
    ],
    "where_eq": "p.pirate_id = \"1\""
  }' \
  localhost:50051 yuaidb.DatabaseService/ExecuteQuery
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

