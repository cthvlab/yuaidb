use std::io::{self, Write};
use tokio;
use yuaidb::Database;

#[tokio::main]
async fn main() {
    let db = Database::new("./data", "./config.toml").await;
    println!("Привет, юный пират! Это интерактивное управление базой данных. Доступные команды:");
    println!("Вставить: insert pirates name:Джек ship_id:101");
    println!("Выбрать: select p.name, s.name from pirates as p join ships as s on s.ship_id = p.ship_id order by p.name desc limit 2 offset 1");
    println!("Обновить: update pirates set ship_id:102 where name = Джек");
    println!("Удалить: delete from pirates where ship_id = 101 limit 1");
    println!("- exit (для выхода)");

    // Пакетная вставка через insert
    db.insert("pirates")
        .values(vec![
            vec![("id", "1"), ("name", "Капитан Джек Воробот"), ("ship_id", "101")],
            vec![("id", "2"), ("name", "Лихой Иван"), ("ship_id", "102")],
            vec![("id", "3"), ("name", "Морской Волк"), ("ship_id", "101")],
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

                let query = db.insert(table).values(vec![fields.clone()]);
                match table {
                    "pirates" | "ships" => {
                        println!("Добавляем в таблицу '{}': {:?}", table, fields);
                        query.execute(&db).await;
                        println!("Данные успешно добавлены!");
                    }
                    _ => println!("Ошибка: неизвестная таблица '{}'. Доступны: pirates, ships", table),
                }
            }
            "select" => {
                if parts.len() < 4 {
                    println!("Ошибка: неверный формат. Используйте: select <fields> from <table> [as <alias>] [join ...] [where ...] [group by ...] [order by ...] [limit <n>] [offset <n>]");
                    continue;
                }

                let from_idx = parts.iter().position(|&p| p == "from");
                if from_idx.is_none() || from_idx.unwrap() < 2 {
                    println!("Ошибка: укажите 'from' после полей");
                    continue;
                }
                let from_idx = from_idx.unwrap();

                let fields_str = parts[1..from_idx].join(" ");
                let fields = fields_str.split(',').map(|s| s.trim()).collect::<Vec<&str>>();
                let table = parts[from_idx + 1];
                let mut query = db.select(table).fields(fields);
                let mut i = from_idx + 2;

                while i < parts.len() {
                    let mut step = 1; // По умолчанию шаг 1
                    match parts[i] {
                        "as" => {
                            if i + 1 >= parts.len() {
                                println!("Ошибка: укажите алиас после 'as'");
                                continue;
                            }
                            query = query.alias(parts[i + 1]);
                            step = 2; // 'as' <alias>
                        }
                        "join" => {
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
                            step = 8; // 'join' <table> 'as' <alias> 'on' <on_left> '=' <on_right>
                        }
                        "where" => {
                            if i + 3 >= parts.len() || parts[i + 2] != "=" {
                                println!("Ошибка: неверный формат WHERE. Используйте: where <field> = <value>");
                                continue;
                            }
                            let field = parts[i + 1];
                            let value = parts[i + 3];
                            query = query.where_eq(field, value);
                            step = 4; // 'where' <field> '=' <value>
                        }
                        "group" => {
                            if i + 1 >= parts.len() || parts[i + 1] != "by" {
                                println!("Ошибка: укажите 'by' после 'group'");
                                continue;
                            }
                            if i + 2 >= parts.len() {
                                println!("Ошибка: укажите поле после 'group by'");
                                continue;
                            }
                            let group_field = parts[i + 2];
                            query = query.group_by(group_field);
                            step = 3; // 'group' 'by' <field>
                        }
                        "order" => {
                            if i + 1 >= parts.len() || parts[i + 1] != "by" {
                                println!("Ошибка: укажите 'by' после 'order'");
                                continue;
                            }
                            if i + 2 >= parts.len() {
                                println!("Ошибка: укажите поле после 'order by'");
                                continue;
                            }
                            let order_field = parts[i + 2];
                            let ascending = if i + 3 < parts.len() {
                                match parts[i + 3].to_lowercase().as_str() {
                                    "asc" => true,
                                    "desc" => false,
                                    _ => true,
                                }
                            } else {
                                true
                            };
                            query = query.order_by(order_field, ascending);
                            step = if i + 3 < parts.len() && (parts[i + 3].to_lowercase() == "asc" || parts[i + 3].to_lowercase() == "desc") { 4 } else { 3 };
                        }
                        "limit" => {
                            if i + 1 >= parts.len() {
                                println!("Ошибка: укажите число после 'limit'");
                                continue;
                            }
                            if let Ok(limit) = parts[i + 1].parse::<usize>() {
                                query = query.limit(limit);
                                step = 2; // 'limit' <number>
                            } else {
                                println!("Ошибка: 'limit' должен быть числом");
                                continue;
                            }
                        }
                        "offset" => {
                            if i + 1 >= parts.len() {
                                println!("Ошибка: укажите число после 'offset'");
                                continue;
                            }
                            if let Ok(offset) = parts[i + 1].parse::<usize>() {
                                query = query.offset(offset);
                                step = 2; // 'offset' <number>
                            } else {
                                println!("Ошибка: 'offset' должен быть числом");
                                continue;
                            }
                        }
                        _ => {
                            println!("Ошибка: неизвестный параметр '{}'", parts[i]);
                        }
                    }
                    i += step; // Обновляем i только здесь
                }

                let query_clone = query.clone();
                if let Some(rows) = query_clone.execute(&db).await {
                    if query.group_by.is_some() {
                        let group_field = query.group_by.as_ref().unwrap().clone();
                        for row in rows {
                            println!(
                                "Группа {}: {} записей",
                                row.get(&group_field).unwrap_or(&"неизвестно".to_string()),
                                row.get("count").unwrap_or(&"0".to_string())
                            );
                        }
                    } else {
                        for row in rows {
                            println!("{:?}", row);
                        }
                    }
                } else {
                    println!("Нет данных для отображения.");
                }
            }
            "update" => {
                if parts.len() < 4 || parts[2] != "set" {
                    println!("Ошибка: неверный формат. Используйте: update <table> set <field1:value1> <field2:value2> ... [where ...]");
                    continue;
                }

                let table = parts[1];
                let mut i = 3;
                let fields_end = parts[i..].iter().position(|&p| p == "where").unwrap_or(parts.len() - i);
                let fields: Vec<(&str, &str)> = parts[i..i + fields_end]
                    .iter()
                    .filter_map(|part| part.split_once(':'))
                    .collect();

                if fields.is_empty() {
                    println!("Ошибка: неверный формат полей. Используйте <field>:<value>");
                    continue;
                }

                let mut query = db.update(table).values(vec![fields.clone()]);
                i += fields_end;

                while i < parts.len() {
                    let mut step = 1;
                    match parts[i] {
                        "where" => {
                            if i + 3 >= parts.len() || parts[i + 2] != "=" {
                                println!("Ошибка: неверный формат WHERE. Используйте: where <field> = <value>");
                                continue;
                            }
                            let field = parts[i + 1];
                            let value = parts[i + 3];
                            query = query.where_eq(field, value);
                            step = 4; // 'where' <field> '=' <value>
                        }
                        _ => {
                            println!("Ошибка: неизвестный параметр '{}'", parts[i]);
                        }
                    }
                    i += step;
                }

                match table {
                    "pirates" | "ships" => {
                        println!("Обновляем в таблице '{}': {:?}", table, fields);
                        query.execute(&db).await;
                        println!("Данные успешно обновлены!");
                    }
                    _ => println!("Ошибка: неизвестная таблица '{}'. Доступны: pirates, ships", table),
                }
            }
            "delete" => {
                if parts.len() < 3 || parts[1] != "from" {
                    println!("Ошибка: неверный формат. Используйте: delete from <table> [where ...] [limit <n>] [offset <n>]");
                    continue;
                }

                let table = parts[2];
                let mut query = db.delete(table);
                let mut i = 3;

                while i < parts.len() {
                    let mut step = 1;
                    match parts[i] {
                        "where" => {
                            if i + 3 >= parts.len() || parts[i + 2] != "=" {
                                println!("Ошибка: неверный формат WHERE. Используйте: where <field> = <value>");
                                continue;
                            }
                            let field = parts[i + 1];
                            let value = parts[i + 3];
                            query = query.where_eq(field, value);
                            step = 4; // 'where' <field> '=' <value>
                        }
                        "limit" => {
                            if i + 1 >= parts.len() {
                                println!("Ошибка: укажите число после 'limit'");
                                continue;
                            }
                            if let Ok(limit) = parts[i + 1].parse::<usize>() {
                                query = query.limit(limit);
                                step = 2; // 'limit' <number>
                            } else {
                                println!("Ошибка: 'limit' должен быть числом");
                                continue;
                            }
                        }
                        "offset" => {
                            if i + 1 >= parts.len() {
                                println!("Ошибка: укажите число после 'offset'");
                                continue;
                            }
                            if let Ok(offset) = parts[i + 1].parse::<usize>() {
                                query = query.offset(offset);
                                step = 2; // 'offset' <number>
                            } else {
                                println!("Ошибка: 'offset' должен быть числом");
                                continue;
                            }
                        }
                        _ => {
                            println!("Ошибка: неизвестный параметр '{}'", parts[i]);
                        }
                    }
                    i += step;
                }

                match table {
                    "pirates" | "ships" => {
                        println!("Удаляем из таблицы '{}'", table);
                        query.execute(&db).await;
                        println!("Данные успешно удалены!");
                    }
                    _ => println!("Ошибка: неизвестная таблица '{}'. Доступны: pirates, ships", table),
                }
            }
            "exit" => {
                println!("Выход...");
                break;
            }
            _ => println!("Неизвестная команда: {}. Доступны: insert, select, update, delete, exit", parts[0]),
        }
    }

    // Финальный запрос для проверки всех данных
    println!("\nТекущие пираты и их корабли (сортировка по имени пирата DESC, limit 2, offset 1):");
    if let Some(rows) = db.select("pirates")
        .alias("p")
        .fields(vec!["p.name", "s.name", "s.speed"])
        .join("ships", "s", "s.ship_id", "p.ship_id")
        .order_by("p.name", false)
        .limit(2)
        .offset(1)
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

    // Пример с GROUP BY и LIMIT
    println!("\nКоличество пиратов на каждом корабле (limit 1):");
    if let Some(rows) = db.select("pirates")
        .alias("p")
        .fields(vec!["s.name"])
        .join("ships", "s", "s.ship_id", "p.ship_id")
        .group_by("s.name")
        .limit(1)
        .execute(&db)
        .await
    {
        for row in rows {
            println!(
                "Корабль {}: {} пиратов",
                row.get("s.name").unwrap(),
                row.get("count").unwrap_or(&"0".to_string())
            );
        }
    }
}
