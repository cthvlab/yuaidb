use std::io::{self, Write};
use tokio;
use yuaidb::Database;

#[tokio::main]
async fn main() {
    let db = Database::new("./data", "./config.toml").await;
    println!("Интерактивное добавление данных в базу. Доступные команды:");
    println!("- add <table> <field1:value1> <field2:value2> ... (например, add pirates pirate_id:1 name:Джек ship_id:101)");
    println!("- select <fields> from <table> [as <alias>] [join <join_table> as <join_alias> on <on_left> = <on_right>] (например, select * from pirates или select p.name, s.name from pirates as p join ships as s on s.ship_id = p.ship_id)");
    println!("- exit (для выхода)");

    loop {
        print!("> ");
        io::stdout().flush().unwrap();

        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();
        let input = input.trim();

        if input.is_empty() {
            continue;
        }

        let parts: Vec<&str> = input.split_whitespace().collect();
        match parts[0] {
            "add" => {
                if parts.len() < 3 {
                    println!("Ошибка: укажите таблицу и хотя бы одно поле (например, add pirates pirate_id:1)");
                    continue;
                }

                let table = parts[1];
                let fields: Vec<(&str, &str)> = parts[2..]
                    .iter()
                    .filter_map(|part| part.split_once(':'))
                    .collect();

                if fields.is_empty() {
                    println!("Ошибка: неверный формат полей. Используйте <field>:<value>");
                    continue;
                }

                match table {
                    "pirates" | "ships" => {
                        println!("Добавляем в таблицу '{}': {:?}", table, fields);
                        db.insert(table)
                            .values(fields)
                            .execute(&db)
                            .await;
                        println!("Данные успешно добавлены!");
                    }
                    _ => println!("Ошибка: неизвестная таблица '{}'. Доступны: pirates, ships", table),
                }
            }
            "select" => {
                if parts.len() < 4 || parts[2] != "from" {
                    println!("Ошибка: неверный формат. Используйте: select <fields> from <table> [as <alias>] [join ...]");
                    continue;
                }

                let fields = parts[1].split(',').map(|s| s.trim()).collect::<Vec<&str>>();
                let table = parts[3];
                let mut query = db.select(table).fields(fields);

                // Парсим опциональный алиас
                let mut i = 4;
                if i < parts.len() && parts[i] == "as" {
                    if i + 1 >= parts.len() {
                        println!("Ошибка: укажите алиас после 'as'");
                        continue;
                    }
                    query = query.alias(parts[i + 1]);
                    i += 2;
                }

                // Парсим опциональный JOIN
                if i < parts.len() && parts[i] == "join" {
                    if i + 5 >= parts.len() || parts[i + 2] != "as" || parts[i + 4] != "on" {
                        println!("Ошибка: неверный формат JOIN. Используйте: join <table> as <alias> on <on_left> = <on_right>");
                        continue;
                    }
                    let join_table = parts[i + 1];
                    let join_alias = parts[i + 3];
                    let on_left = parts[i + 5];
                    if i + 6 >= parts.len() || parts[i + 6] != "=" {
                        println!("Ошибка: укажите условие JOIN в формате <on_left> = <on_right>");
                        continue;
                    }
                    let on_right = parts[i + 7];
                    query = query.join(join_table, join_alias, on_left, on_right);
                    i += 8;
                }

                // Выполняем запрос
                if let Some(rows) = query.execute(&db).await {
                    for row in rows {
                        println!("{:?}", row);
                    }
                } else {
                    println!("Нет данных для отображения.");
                }
            }
            "exit" => {
                println!("Выход...");
                break;
            }
            _ => println!("Неизвестная команда: {}. Доступны: add, select, exit", parts[0]),
        }
    }

    // Финальный запрос для проверки всех данных
    println!("\nТекущие пираты и их корабли:");
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
                row.get("s.speed").unwrap_or(&"неизвестно".to_string())
            );
        }
    }
}
