# YUAIDB: Your Uberfast Async In-memory DataBase 🚀✨

Добро пожаловать в **YUAIDB** — базу данных, которая живёт в оперативке, выполняет SQL Like запросы как Postgres, уделывает Redis в скорости и сохраняет ваши данные на диск быстрее, чем вы скажете "Где мой заказ?". Написана на Rust, потому что мы любим быстрые, безопасные и простые штуки. 😎

## Что это такое? 🤔

YUAIDB — это in-memory база данных для **актуальных данных**, где важна молниеносная скорость чтения и записи. Мы созданы для "горячих" штук вроде пользовательских сессий, заказов в работе, игровых состояний или любых данных, которые нужно обновлять и получать на лету. 

- **Скорость**: O(1) операции в памяти, плюс параллелизм через `tokio`.
- **Надёжность**: WAL O(1) -> .bin сразу на диск — никаких потерь при сбоях.
- **Гибкость**: Числа, Текст, Метки времени, Булевы значения, JOIN, WHERE, ORDER BY, GROUP BY и полнотекстовый поиск!
- **Режимы**: Встраиваемая библиотека или gRPC-сервис — выбирайте свой стиль.
- **Кроссплатформенность**: Работает на всех ОС — Windows, Linux, macOS


## Зачем это нужно? 🎉

- Хотите базу для актуальных данных быстрее Redis? YUAIDB в деле.
- Нужна железная сохранность для сессий, заказов или игровых очков? Мы пишем на диск после каждого чиха.
- У вас не так много данных? Это Postgres в оперативке!
- Любите Rust и хотите базу с характером? Вы по адресу!

**Важно**: YUAIDB — не про терабайты и кластеры. Для этого берите базы с масштабированием или добавьте такие фичи в YUAIDB сами — код открыт!

## Как это работает? 🛠️

YUAIDB держит добычу в памяти с `DashMap` — это турбо-скорость и многопоточность без багов. После каждого манёвра (`insert`, `update`, `delete`) данные пишутся в журнал и каждые 60 секунд сливаются в `.bin` файлы на диск — никаких "ой, забыл сохранить". 
Перезапуск? Всё грузится обратно в RAM, как по волшебству.

### Ключевые фичи:
- **O(1) в памяти**: Чистая скорость операций без записи на диск.
- **Сохранение на диск**: Чтобы ни один заказ или сессия не потерялись.
- **Индексы**: Обычные и полнотекстовые — ищите как профи.
- **JOIN-ы**: Параллельные, с кэшем — для сложных запросов.
- **gRPC**: Сервисный режим для тех, кто хочет управлять через крутой протокол.
- **Типы полей**: (`numeric`, `text`, `timestamp`, `boolean`) и сортировка через `ORDER BY`.

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

3. **Пример конфига** (`config.toml`):
   ```toml
   [[tables]]
   name = "pirates"
   [[tables.fields]]
   name = "id"
   field_type = "numeric"
   indexed = true
   autoincrement = true
   unique = true
   [[tables.fields]]
   name = "name"
   field_type = "text"
   [[tables.fields]]
   name = "ship_id"
   field_type = "numeric"
   
   [[tables]]
   name = "ships"
   [[tables.fields]]
   name = "ship_id"
   field_type = "numeric"
   indexed = true
   autoincrement = true
   unique = true
   [[tables.fields]]
   name = "name"
   field_type = "text"
   [[tables.fields]]
   name = "speed"
   field_type = "numeric"
   ```

## Пример использования 💻

### Встраиваемый режим
```rust
use yuaidb::Database;

#[tokio::main]
async fn main() {
   let db = Database::new("./data", "./config.toml").await;
   db.insert("pirates")
        .values(vec![
            vec![("name", "Капитан Джек Воробот"), ("ship_id", "101")],
            vec![("name", "Лихой Иван"), ("ship_id", "102")],
        ])
        .execute(&db)
        .await;

    db.insert("ships")
        .values(vec![
            vec![("ship_id", "101"), ("name", "Чёрная Комета"), ("speed", "0.9c")],
            vec![("ship_id", "102"), ("name", "Астероидный Шторм"), ("speed", "0.7c")],
        ])
        .execute(&db)
        .await;

    println!("Кто на чём летает:");
    if let Some(rows) = db.select("pirates")
        .alias("p")
        .fields(vec!["p.name", "s.name", "s.speed"])
        .join("ships", "s", "s.ship_id", "p.ship_id")
        .order_by("s.speed", false) // Добавлена сортировка по скорости (DESC)
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
        .where_eq("id", "1")
        .execute(&db)
        .await;

    println!("\nПосле смены корабля:");
    if let Some(rows) = db.select("pirates")
        .alias("p")
        .fields(vec!["p.name", "s.name"])
        .join("ships", "s", "s.ship_id", "p.ship_id")
        .where_eq("p.id", "1")
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
SELECT p.name, s.name FROM pirates p JOIN ships s ON s.ship_id = p.ship_id
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
    "where_eq": "p.id = \"1\""
  }' \
  localhost:50051 yuaidb.DatabaseService/ExecuteQuery
```

## Сравнение с Redis 😏

| Фича             | YUAIDB (без диска) | YUAIDB (с диском) | Redis (без AOF) | Redis (AOF every write) |
|------------------|--------------------|-------------------|-----------------|-------------------------|
| **Скорость записи** | ~1.2M оп/сек       | ~666k оп/сек      | ~1M оп/сек      | ~50k–100k оп/сек        |
| **Скорость чтения** | ~1.1M оп/сек       | ~1.1M оп/сек      | ~1M оп/сек      | ~1M оп/сек              |
| **JOIN и WHERE**    | Есть!              | Есть!             | Нет             | Нет                     |
| **ORDER BY**        | Есть (по типам!)   | Есть (по типам!)  | Нет             | Нет                     |
| **Типы данных**     | Numeric, Text, etc.| Numeric, Text, etc.| Строки          | Строки                  |
| **Сохранение**      | Нет                | Каждую операцию   | Нет             | Каждую операцию (AOF)   |
| **Надёжность**     | Потеря при сбое    | 100% сохранность  | Потеря при сбое | 100% сохранность       |

### Пояснения:
- **YUAIDB (без диска)**: Чистая O(1) скорость в памяти — обгоняем Redis за счёт параллелизма и встраиваемости.
- **YUAIDB (с диском)**: Синхронная запись на диск замедляет, но спасает актуальные данные (сессии, очки, заказы).
- **Redis (без AOF)**: Быстро, но без сохранности — шторм унесёт всё!
- **Redis (AOF every write)**: Надёжно, но медленно — черепаха против нашего фрегата.

**Вывод**: YUAIDB в памяти быстрее Redis, с диском — идеальный баланс скорости и сохранности, а с типами и сортировкой — настоящий пиратский флот для игр и приложений!

## Лицензия 📜

MIT — берите, используйте, веселитесь!

YUAIDB — это ваш билет в мир скорости, надёжности и Rust-магии для актуальных данных, игр и приложений. Попробуйте, и пусть ваши заказы и сессии летают как ракета! 🚀

## Авторы

Разработано сообществом ЮАИ [yuai.ru](https://yuai.ru) 
