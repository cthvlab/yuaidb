use yuaidb::{Database, Row, Condition};
use std::collections::HashMap;
use rand::Rng;

#[tokio::main]
async fn main() {
    // Инициализация базы данных
    let db = Database::new("./data", "./dbconfig.toml").await;

    // Создание таблицы
    db.create_table("users").await;
    println!("Таблица 'users' создана.");

    // Вставка данных
    let mut rng = rand::thread_rng();
    let row1 = Row {
        id: rng.gen::<i32>(),
        data: HashMap::from([
            ("name".to_string(), "Alice".to_string()),
            ("age".to_string(), "25".to_string()),
        ]),
    };
    let row2 = Row {
        id: rng.gen::<i32>(),
        data: HashMap::from([
            ("name".to_string(), "Bob".to_string()),
            ("age".to_string(), "30".to_string()),
        ]),
    };

    db.insert("users", row1).await;
    db.insert("users", row2).await;
    println!("Данные вставлены.");

    // Выборка данных
    let conditions = vec![Condition::Eq("age".to_string(), "25".to_string())];
    if let Some(rows) = db.select("users", conditions, None, None).await {
        println!("Найденные строки: {:?}", rows);
    } else {
        println!("Таблица 'users' не найдена.");
    }

    // Обновление данных
    let updates = HashMap::from([("age".to_string(), "26".to_string())]);
    db.update("users", vec![Condition::Eq("name".to_string(), "Alice".to_string())], updates).await;
    println!("Данные обновлены.");

    // Удаление данных
    db.delete("users", vec![Condition::Eq("name".to_string(), "Bob".to_string())]).await;
    println!("Данные удалены.");

    // Проверка оставшихся данных
    if let Some(rows) = db.select("users", vec![], None, None).await {
        println!("Оставшиеся строки: {:?}", rows);
    }
}