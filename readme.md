# YUAIDB: Your Uberfast Async In-memory DataBase 🚀✨

Добро пожаловать в **YUAIDB** — базу данных, которая живёт в оперативке, выполняет SQL Like запросы как Postgres, уделывает Redis в скорости и сохраняет ваши данные на диск быстрее, чем вы скажете "Где мой заказ?". Написана на Rust, потому что мы любим быстрые, безопасные и простые штуки. 😎

## Что это такое? 🤔

YUAIDB — это in-memory база данных для **актуальных данных**, где важна молниеносная скорость чтения и записи. Мы созданы для "горячих" штук вроде пользовательских сессий, заказов в работе, игровых состояний или любых данных, которые нужно обновлять и получать на лету. 

- **Скорость**: O(1) операции в памяти, плюс параллелизм через `tokio`.
- **Надёжность**: WAL O(1) -> .bin сразу на диск — никаких потерь при сбоях.
- **Гибкость**: Числа, Текст, Метки времени, Булевы значения, JOIN, WHERE, ORDER BY, GROUP BY и полнотекстовый поиск!
- **Режимы**: Встраиваемая библиотека или gRPC-сервис — выбирайте свой стиль.
- **Кроссплатформенность**: Работает на всех операционных системах


## Зачем это нужно? 🎉

- Хотите базу для актуальных данных быстрее Redis? YUAIDB в деле.
- Нужна железная сохранность для сессий, заказов или игровых очков? Мы пишем на диск после каждого чиха.
- У вас не так много данных? Это Postgres в оперативке!
- Любите Rust и хотите базу с характером? Вы по адресу!

**Важно**: YUAIDB — не про терабайты и кластеры. Для этого берите базы с масштабированием или добавьте такие фичи в YUAIDB сами — код открыт!

## Как это работает? 🛠️

YUAIDB держит добычу в памяти с `DashMap` — это турбо-скорость и многопоточность без багов. После каждого манёвра (`insert`, `update`, `delete`) данные пишутся в журнал и каждые 10 секунд сливаются в `.bin` файлы на диск — никаких "ой, забыл сохранить". 
Перезапуск? Всё грузится обратно в RAM, как по волшебству.

### Ключевые фичи:
- **O(1) в памяти**: Чистая скорость операций без записи на диск.
- **Сохранение на диск**: Чтобы ни один заказ или сессия не потерялись.
- **Индексы**: Обычные и полнотекстовые — ищите как профи.
- **JOIN-ы**: Параллельные, с кэшем — для сложных запросов.
- **gRPC**: Сервисный режим для тех, кто хочет управлять через крутой протокол.
- **Типы полей**: (`numeric`, `text`, `timestamp`, `boolean`) и сортировка через `ORDER BY`.

## Установка и запуск 🎬
0. **Установите RUST**:
   ```bash
   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
   ```
   
2. **Клонируйте репозиторий**:
   ```bash
   git clone https://github.com/cthvlab/yuaidb.git
   cd yuaidb
   ```

3. **Запустите как встраиваемую базу**:
   ```bash
   cargo run
   ```

4. **Пример конфига** (`config.toml`):
   ```toml
   [[tables]]
   name = "pirates"
   [[tables.fields]]
   name = "id"
   field_type = "numeric"
   autoincrement = true
   unique = true
   indexed = true
   [[tables.fields]]
   name = "name"
   field_type = "text"
   indexed = true
   fulltext = true
   [[tables.fields]]
   name = "ship_id"
   field_type = "numeric"
   
   [[tables]]
   name = "ships"
   [[tables.fields]]
   name = "ship_id"
   field_type = "numeric"
   autoincrement = true
   unique = true
   indexed = true
   [[tables.fields]]
   name = "name"
   field_type = "text"
   indexed = true
   fulltext = true
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
    // Инициализируем базу данных 
    let db = match Database::new("./data", "./config.toml").await {
        Ok(db) => db,
        Err(e) => {
            println!("Ошибка при запуске базы: {}", e);
            return;
        }
    };

    // Вставляем пиратов
    let insert_pirates = db.insert("pirates")
        .values(vec![
            vec![("name", "Капитан Джек Воробот"), ("ship_id", "101")],
            vec![("name", "Лихой Иван"), ("ship_id", "102")],
            vec![("name", "Морской Волк"), ("ship_id", "101")],
            vec![("name", "Корм для рыб"), ("ttl", "60")], // Будет выброшен за борт через 60 сек
        ]);
    if let Err(e) = insert_pirates.execute(&db).await {
        println!("Ошибка при вставке пиратов: {}", e);
    } else {
        println!("Пираты успешно добавлены!");
    }

    // Вставляем корабли
    let insert_ships = db.insert("ships")
        .values(vec![
            vec![("ship_id", "101"), ("name", "Чёрная Комета"), ("speed", "0.9")],
            vec![("ship_id", "102"), ("name", "Астероидный Шторм"), ("speed", "0.7")],
        ]);
    if let Err(e) = insert_ships.execute(&db).await {
        println!("Ошибка при вставке кораблей: {}", e);
    } else {
        println!("Корабли успешно добавлены!");
    }

    // Запрос 1: SELECT с JOIN, WHERE (OR и AND), ORDER BY, LIMIT и OFFSET
    println!("\nЗапрос 1: Пираты с кораблями (сборный фильтр):");
    let complex_select = db.select("pirates")
        .alias("p")
        .fields(vec!["p.name", "s.name", "s.speed"])
        .join("ships", "s", "s.ship_id", "p.ship_id")
        .where_contains("p.name", "Капитан") // Имя содержит "Капитан"
        .where_eq("s.speed", "0.9")          // Скорость точно 0.9
        .where_in("p.ship_id", vec!["101", "102"]) // Корабль в списке
        .order_by("s.speed", false)          // Сортировка по убыванию скорости
        .limit(1)                            // Только 1 запись
        .offset(0);                          // С начала
    match complex_select.execute(&db).await {
        Ok(Some(rows)) => {
            for row in rows {
                println!(
                    "Пират {} управляет кораблём {} со скоростью {}",
                    row.get("p.name").unwrap_or(&"Неизвестный".to_string()),
                    row.get("s.name").unwrap_or(&"Безымянный".to_string()),
                    row.get("s.speed").unwrap_or(&"0".to_string())
                );
            }
        }
        Ok(None) => println!("Ничего не найдено! Пираты сбежали?"),
        Err(e) => println!("Ошибка при запросе: {}", e),
    }

    // Запрос 2: UPDATE с BETWEEN и WHERE
    println!("\nЗапрос 2: Обновляем ship_id для пиратов с именами, содержащими 'Иван' или 'Волк':");
    let complex_update = db.update("pirates")
        .values(vec![("ship_id", "102")]) // Всех переводим на "Астероидный Шторм"
        .where_contains("name", "Иван")   // Имя содержит "Иван"
        .where_contains("name", "Волк");  // ИЛИ "Волк"
    if let Err(e) = complex_update.execute(&db).await {
        println!("Ошибка при обновлении: {}", e);
    } else {
        println!("Пираты успешно переведены на новый корабль!");
    }

    // Запрос 3: DELETE с BETWEEN и WHERE
    println!("\nЗапрос 3: Удаляем пиратов с кораблями speed > 0.8 ИЛИ name содержит 'Джек':");
    let complex_delete = db.delete("pirates")
        .where_gt("ship_id", "100")        // ship_id > 100 
        .where_contains("name", "Джек");   // ИЛИ имя содержит "Джек"
    if let Err(e) = complex_delete.execute(&db).await {
        println!("Ошибка при удалении: {}", e);
    } else {
        println!("Пираты выброшены за борт!");
    }

    // Запрос 4: Финальный SELECT с JOIN и проверкой результата
    println!("\nЗапрос 4: Проверяем оставшихся пиратов:");
    let final_select = db.select("pirates")
        .alias("p")
        .fields(vec!["p.name", "s.name", "s.speed"])
        .join("ships", "s", "s.ship_id", "p.ship_id")
        .order_by("p.name", true) // Сортировка по имени по возрастанию
        .limit(10);               // Лимит на всякий случай
    match final_select.execute(&db).await {
        Ok(Some(rows)) => {
            for row in rows {
                println!(
                    "Остался пират {} на корабле {} со скоростью {}",
                    row.get("p.name").unwrap_or(&"Неизвестный".to_string()),
                    row.get("s.name").unwrap_or(&"Безымянный".to_string()),
                    row.get("s.speed").unwrap_or(&"0".to_string())
                );
            }
        }
        Ok(None) => println!("Все пираты сбежали или удалены!"),
        Err(e) => println!("Ошибка при запросе: {}", e),
    }
}
```
#### SQL-подобный синтаксис:
```SQL
.select("pirates").alias("p").fields(vec!["p.name", "s.name"]).join("ships", "s", "s.ship_id", "p.ship_id") 
SELECT p.name, s.name FROM pirates AS p JOIN ships AS s ON s.ship_id = p.ship_id
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
