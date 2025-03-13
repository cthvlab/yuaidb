use std::io::{self, Write};
use tokio;
use yuaidb::Database;

#[tokio::main]
async fn main() {
    let db = Database::new("./data", "./config.toml").await;
    println!("Интерактивное добавление данных в базу. Доступные команды:");
    println!("- insert <table> <field1:value1> <field2:value2> ... (например, insert pirates name:Джек ship_id:101)");
    println!("- select <fields> from <table> [as <alias>] [join <join_table> as <join_alias> on <on_left> = <on_right>]");
    println!("- exit (для выхода)");

    // Пакетная вставка через insert
    db.insert("pirates")
        .values(vec![
            vec![("id", "1"), ("name", "Капитан Джек Воробот"), ("ship_id", "101")],
            vec![("id", "2"),("name", "Лихой Билл"), ("ship_id", "102")],
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
            "insert" => {
                if parts.len() < 3 {
                    println!("Ошибка: укажите таблицу и хотя бы одно поле (например, insert pirates name:Джек)");
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
                            .values(vec![fields]) // Одиночная запись оборачивается в vec!
                            .execute(&db)
                            .await;
                        println!("Данные успешно добавлены!");
                    }
                    _ => println!("Ошибка: неизвестная таблица '{}'. Доступны: pirates, ships", table),
                }
            }
            "select" => {
                if parts.len() < 4 || parts[2] != "from" {
                    println!("Ошибка: неверный формат. Используйте: select <fields> from <table> [as <alias>] [join ...] [where ...]");
                    continue;
                }

                let fields = parts[1].split(',').map(|s| s.trim()).collect::<Vec<&str>>();
                let table = parts[3];
                let mut query = db.select(table).fields(fields);

                let mut i = 4;
                if i < parts.len() && parts[i] == "as" {
                    if i + 1 >= parts.len() {
                        println!("Ошибка: укажите алиас после 'as'");
                        continue;
                    }
                    query = query.alias(parts[i + 1]);
                    i += 2;
                }

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
                    i += 8; // Теперь i используется дальше
                }

                if i < parts.len() && parts[i] == "where" {
                    if i + 3 >= parts.len() || parts[i + 2] != "=" {
                        println!("Ошибка: неверный формат WHERE. Используйте: where <field> = <value>");
                        continue;
                    }
                    let field = parts[i + 1];
                    let value = parts[i + 3];
                    query = query.where_eq(field, value);
                    // i += 4; // Можно добавить, если будут ещё условия
                }

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
