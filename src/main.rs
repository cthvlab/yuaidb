use std::io::{self, Write};
use tokio;
use yuaidb::{Database, Condition, Query};
use colored::*;

// Парсим поля вида <field>:<value>, где value может быть длинным текстом в кавычках
fn parse_fields(parts: &[&str]) -> Result<Vec<(String, String)>, String> {
    let mut fields = Vec::new();
    let mut i = 0;

    while i < parts.len() {
        if let Some((field, value_start)) = parts[i].split_once(':') {
            let mut value = String::new();
            let start_with_quote = value_start.starts_with('"');

            if start_with_quote {
                if value_start.len() > 1 {
                    value.push_str(&value_start[1..]);
                }
                i += 1;
                while i < parts.len() {
                    let part = parts[i];
                    if part.ends_with('"') {
                        value.push_str(" ");
                        value.push_str(&part[..part.len() - 1]);
                        i += 1;
                        break;
                    } else {
                        value.push_str(" ");
                        value.push_str(part);
                        i += 1;
                    }
                }
                if i >= parts.len() && !parts[parts.len() - 1].ends_with('"') {
                    return Err("Ошибка: незакрытая кавычка в значении".to_string());
                }
            } else {
                value.push_str(value_start);
                i += 1;
            }

            fields.push((field.to_string(), value.trim().to_string()));
        } else {
            i += 1;
        }
    }

    if fields.is_empty() {
        Err("Ошибка: не найдены поля в формате <field>:<value>".to_string())
    } else {
        Ok(fields)
    }
}

// Парсим условия WHERE
fn parse_where(parts: &[&str], i: &mut usize, query: &mut Query) -> Result<(), String> {
    let mut current_group = Vec::new();

    while *i < parts.len() {
        let part = parts[*i].to_uppercase();
        let is_where_or_logical = part == "WHERE" || part == "OR" || part == "AND";

        if is_where_or_logical {
            if part == "OR" && !current_group.is_empty() {
                query.where_clauses.push(current_group);
                current_group = Vec::new();
            }
            *i += 1;

            if *i >= parts.len() {
                return Err(format!("Ошибка: укажите условие после '{}'", part.to_lowercase()));
            }

            let field = parts[*i];
            *i += 1;

            if *i >= parts.len() {
                return Err(format!("Ошибка: укажите оператор после поля '{}'", field));
            }

            let operator = parts[*i].to_uppercase();
            *i += 1;

            if *i >= parts.len() {
                return Err(format!("Ошибка: укажите значение после оператора '{}'", operator));
            }

            match operator.as_str() {
                "=" => {
                    let value = parse_value(parts, i)?;
                    current_group.push(Condition::Eq(field.to_string(), value));
                }
                "<" => {
                    let value = parse_value(parts, i)?;
                    current_group.push(Condition::Lt(field.to_string(), value));
                }
                ">" => {
                    let value = parse_value(parts, i)?;
                    current_group.push(Condition::Gt(field.to_string(), value));
                }
                "CONTAINS" => {
                    let value = parse_value(parts, i)?;
                    current_group.push(Condition::Contains(field.to_string(), value));
                }
                "IN" => {
                    if !parts[*i].starts_with('(') {
                        return Err("Ошибка: ожидается '(' после IN".to_string());
                    }
                    let values = parse_in_values(parts, i)?;
                    if values.is_empty() {
                        return Err("Ошибка: укажите значения в IN".to_string());
                    }
                    current_group.push(Condition::In(field.to_string(), values));
                }
                "BETWEEN" => {
                    let min = parse_value(parts, i)?;
                    if *i >= parts.len() || parts[*i].to_uppercase() != "AND" {
                        return Err("Ошибка: ожидается 'AND' после первого значения BETWEEN".to_string());
                    }
                    *i += 1;
                    if *i >= parts.len() {
                        return Err("Ошибка: укажите второе значение после 'AND' в BETWEEN".to_string());
                    }
                    let max = parse_value(parts, i)?;
                    current_group.push(Condition::Between(field.to_string(), min, max));
                }
                _ => return Err(format!("Ошибка: неизвестный оператор '{}'", operator)),
            }
        } else if query.where_clauses.is_empty() && current_group.is_empty() {
            return Err(format!("Ошибка: ожидается 'where' в начале условия, найдено '{}'", parts[*i]));
        } else {
            break;
        }
    }

    if !current_group.is_empty() {
        query.where_clauses.push(current_group);
    }

    if query.where_clauses.is_empty() {
        return Err("Ошибка: условие WHERE не содержит корректных условий".to_string());
    }

    Ok(())
}

// Парсим значение с поддержкой длинных текстов
fn parse_value(parts: &[&str], i: &mut usize) -> Result<String, String> {
    let mut value = String::new();

    if parts[*i].starts_with('"') {
        value.push_str(&parts[*i][1..]);
        *i += 1;
        while *i < parts.len() {
            let part = parts[*i];
            if part.ends_with('"') {
                value.push_str(" ");
                value.push_str(&part[..part.len() - 1]);
                *i += 1;
                break;
            } else if part.to_uppercase() == "AND" || part.to_uppercase() == "OR" {
                break;
            } else {
                value.push_str(" ");
                value.push_str(part);
                *i += 1;
            }
        }
        if *i >= parts.len() && !parts[*i - 1].ends_with('"') {
            return Err("Ошибка: незакрытая кавычка в значении".to_string());
        }
    } else {
        value = parts[*i].to_string();
        *i += 1;
    }

    Ok(value.trim().to_string())
}

// Парсим значения для IN
fn parse_in_values(parts: &[&str], i: &mut usize) -> Result<Vec<String>, String> {
    let mut values = Vec::new();
    let mut j = *i + 1;

    while j < parts.len() && !parts[j].ends_with(')') {
        if !parts[j].is_empty() && parts[j] != "," {
            values.push(parts[j].trim_matches('"').to_string());
        }
        j += 1;
    }
    if j < parts.len() && parts[j].ends_with(')') {
        let last = parts[j][..parts[j].len() - 1].trim_matches('"');
        if !last.is_empty() {
            values.push(last.to_string());
        }
        *i = j + 1;
    } else {
        return Err("Ошибка: незакрытая скобка в IN".to_string());
    }

    Ok(values)
}

#[tokio::main]
async fn main() {
    let db = Database::new("./data", "./config.toml").await;
    println!("{}", "Привет, юный пират! Это интерактивное управление базой данных. Доступные команды:".purple().bold());
    println!("{}", "Вставить: insert pirates name:\"Капитан Джек Воробот Бла Бла Бла\" ship_id:101".purple());
    println!("{}", "Выбрать: select name from pirates where name contains \"Иван\"".purple());
    println!("{}", "Обновить: update pirates set name:\"Капитан Джек Воробот Новый\" where ship_id = 101".purple());
    println!("{}", "Удалить: delete from pirates where name = \"Капитан Джек Воробот Бла Бла Бла\"".purple());
    println!("{}", "- exit (для выхода)".purple());

    // Инициализация тестовых данных
    let mut query = db.insert("pirates");
    query.values(vec![
        vec![("id", "1"), ("name", "Капитан Джек Воробот"), ("ship_id", "101")],
        vec![("id", "2"), ("name", "Лихой Иван"), ("ship_id", "102")],
        vec![("id", "3"), ("name", "Морской Волк"), ("ship_id", "101")],
    ]);
    if let Err(e) = query.execute(&db).await {
        println!("{}", format!("Ошибка при инициализации pirates: {}", e).yellow());
    }

    let mut query = db.insert("ships");
    query.values(vec![
        vec![("ship_id", "101"), ("name", "Чёрная Комета"), ("speed", "0.9c")],
        vec![("ship_id", "102"), ("name", "Астероидный Шторм"), ("speed", "0.7c")],
    ]);
    if let Err(e) = query.execute(&db).await {
        println!("{}", format!("Ошибка при инициализации ships: {}", e).yellow());
    }

    db.flush_wal_to_bin().await;

    loop {
        print!("{}", "> ".yellow());
        io::stdout().flush().unwrap();

        let mut input = String::new();
        if io::stdin().read_line(&mut input).is_err() {
            println!("{}", "Ошибка чтения ввода!".yellow());
            continue;
        }
        let input = input.trim();
        if input.is_empty() {
            continue;
        }

        let parts: Vec<&str> = input.split_whitespace().collect();

        match parts.get(0).map(|s| s.to_lowercase()).as_deref() {
            Some("insert") => {
                if parts.len() < 3 {
                    println!("{}", "Ошибка: укажите таблицу и хотя бы одно поле (например, insert pirates name:\"Джек\")".yellow());
                    continue;
                }

                let table = parts[1];
                match parse_fields(&parts[2..]) {
                    Ok(fields) => {
                        let fields_ref: Vec<(&str, &str)> = fields.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
                        let mut query = db.insert(table);
                        query.values(fields_ref.clone());
                        match table {
                            "pirates" | "ships" => {
                                println!("{}", format!("Добавляем в таблицу '{}': {:?}", table, fields).green());
                                match query.execute(&db).await {
                                    Ok(_) => {
                                        db.flush_wal_to_bin().await;
                                        println!("{}", "Данные успешно добавлены!".green());
                                    }
                                    Err(e) => println!("{}", format!("Ошибка при вставке: {}", e).yellow()),
                                }
                            }
                            _ => println!("{}", format!("Ошибка: неизвестная таблица '{}'. Доступны: pirates, ships", table).yellow()),
                        }
                    }
                    Err(e) => println!("{}", e.yellow()),
                }
            }
            Some("select") => {
                if parts.len() < 4 {
                    println!("{}", "Ошибка: неверный формат. Используйте: select <fields> from <table> [where ...]".yellow());
                    continue;
                }

                let from_idx = parts.iter().position(|&p| p.to_lowercase() == "from");
                if from_idx.is_none() || from_idx.unwrap() < 2 {
                    println!("{}", "Ошибка: укажите 'from' после полей".yellow());
                    continue;
                }
                let from_idx = from_idx.unwrap();

                let fields_str = parts[1..from_idx].join(" ");
                let fields = fields_str.split(',').map(|s| s.trim()).collect::<Vec<&str>>();
                let table = parts[from_idx + 1];
                let mut query = db.select(table);
                query.fields(fields);
                let mut i = from_idx + 2;

                while i < parts.len() {
                    match parts[i].to_lowercase().as_str() {
                        "as" => {
                            if i + 1 >= parts.len() {
                                println!("{}", "Ошибка: укажите алиас после 'as'".yellow());
                                continue;
                            }
                            query.alias(parts[i + 1]);
                            i += 2;
                        }
                        "join" => {
                            if i + 5 >= parts.len() || parts[i + 2].to_lowercase() != "as" || parts[i + 4].to_lowercase() != "on" {
                                println!("{}", "Ошибка: неверный формат JOIN. Используйте: join <table> as <alias> on <on_left> = <on_right>".yellow());
                                continue;
                            }
                            let join_table = parts[i + 1];
                            let join_alias = parts[i + 3];
                            let on_left = parts[i + 5];
                            if i + 6 >= parts.len() || parts[i + 6] != "=" {
                                println!("{}", "Ошибка: укажите условие JOIN в формате <on_left> = <on_right>".yellow());
                                continue;
                            }
                            let on_right = parts[i + 7];
                            query.join(join_table, join_alias, on_left, on_right);
                            i += 8;
                        }
                        "where" => {
                            if let Err(e) = parse_where(&parts, &mut i, &mut query) {
                                println!("{}", e.yellow());
                                continue;
                            }
                        }
                        "order" => {
                            if i + 1 >= parts.len() || parts[i + 1].to_lowercase() != "by" {
                                println!("{}", "Ошибка: укажите 'by' после 'order'".yellow());
                                continue;
                            }
                            if i + 2 >= parts.len() {
                                println!("{}", "Ошибка: укажите поле после 'order by'".yellow());
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
                            query.order_by(order_field, ascending);
                            i += if i + 3 < parts.len() && (parts[i + 3].to_lowercase() == "asc" || parts[i + 3].to_lowercase() == "desc") { 4 } else { 3 };
                        }
                        "limit" => {
                            if i + 1 >= parts.len() {
                                println!("{}", "Ошибка: укажите число после 'limit'".yellow());
                                continue;
                            }
                            if let Ok(limit) = parts[i + 1].parse::<usize>() {
                                query.limit(limit);
                                i += 2;
                            } else {
                                println!("{}", "Ошибка: 'limit' должен быть числом".yellow());
                                continue;
                            }
                        }
                        "offset" => {
                            if i + 1 >= parts.len() {
                                println!("{}", "Ошибка: укажите число после 'offset'".yellow());
                                continue;
                            }
                            if let Ok(offset) = parts[i + 1].parse::<usize>() {
                                query.offset(offset);
                                i += 2;
                            } else {
                                println!("{}", "Ошибка: 'offset' должен быть числом".yellow());
                                continue;
                            }
                        }
                        _ => {
                            println!("{}", format!("Ошибка: неизвестный параметр '{}'", parts[i]).yellow());
                            continue;
                        }
                    }
                }

                match query.execute(&db).await {
                    Ok(Some(rows)) => {
                        for row in rows {
                            println!("{}", format!("{:?}", row).green());
                        }
                    }
                    Ok(None) => println!("{}", "Нет данных для отображения.".green()),
                    Err(e) => println!("{}", format!("Ошибка при выполнении запроса: {}", e).yellow()),
                }
            }
            Some("update") => {
                if parts.len() < 4 || parts[2].to_lowercase() != "set" {
                    println!("{}", "Ошибка: неверный формат. Используйте: update <table> set <field1>:<value1> ... [where ...]".yellow());
                    continue;
                }

                let table = parts[1];
                let mut i = 3;
                let fields_end = parts[i..].iter().position(|&p| p.to_lowercase() == "where").unwrap_or(parts.len() - i);
                match parse_fields(&parts[i..i + fields_end]) {
                    Ok(fields) => {
                        let fields_ref: Vec<(&str, &str)> = fields.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
                        let mut query = db.update(table);
                        query.values(fields_ref.clone());
                        i += fields_end;

                        if i >= parts.len() {
                            println!("{}", "Ошибка: для команды update требуется условие WHERE".yellow());
                            continue;
                        }

                        if parts[i].to_lowercase() == "where" {
                            if let Err(e) = parse_where(&parts, &mut i, &mut query) {
                                println!("{}", e.yellow());
                                continue;
                            }
                        } else {
                            println!("{}", format!("Ошибка: ожидается 'where', найдено '{}'", parts[i]).yellow());
                            continue;
                        }

                        match table {
                            "pirates" | "ships" => {
                                println!("{}", format!("Обновляем в таблице '{}': {:?}", table, fields).green());
                                match query.execute(&db).await {
                                    Ok(_) => {
                                        db.flush_wal_to_bin().await;
                                        println!("{}", "Данные успешно обновлены!".green());
                                    }
                                    Err(e) => println!("{}", format!("Ошибка при обновлении: {}", e).yellow()),
                                }
                            }
                            _ => println!("{}", format!("Ошибка: неизвестная таблица '{}'. Доступны: pirates, ships", table).yellow()),
                        }
                    }
                    Err(e) => println!("{}", e.yellow()),
                }
            }
            Some("delete") => {
                if parts.len() < 3 || parts[1].to_lowercase() != "from" {
                    println!("{}", "Ошибка: неверный формат. Используйте: delete from <table> [where ...]".yellow());
                    continue;
                }

                let table = parts[2];
                let mut query = db.delete(table);
                let mut i = 3;

                if i >= parts.len() {
                    println!("{}", "Ошибка: для команды delete требуется условие WHERE".yellow());
                    continue;
                }

                if parts[i].to_lowercase() == "where" {
                    if let Err(e) = parse_where(&parts, &mut i, &mut query) {
                        println!("{}", e.yellow());
                        continue;
                    }
                } else {
                    println!("{}", format!("Ошибка: ожидается 'where', найдено '{}'", parts[i]).yellow());
                    continue;
                }

                while i < parts.len() {
                    match parts[i].to_lowercase().as_str() {
                        "limit" => {
                            if i + 1 >= parts.len() {
                                println!("{}", "Ошибка: укажите число после 'limit'".yellow());
                                continue;
                            }
                            if let Ok(limit) = parts[i + 1].parse::<usize>() {
                                query.limit(limit);
                                i += 2;
                            } else {
                                println!("{}", "Ошибка: 'limit' должен быть числом".yellow());
                                continue;
                            }
                        }
                        "offset" => {
                            if i + 1 >= parts.len() {
                                println!("{}", "Ошибка: укажите число после 'offset'".yellow());
                                continue;
                            }
                            if let Ok(offset) = parts[i + 1].parse::<usize>() {
                                query.offset(offset);
                                i += 2;
                            } else {
                                println!("{}", "Ошибка: 'offset' должен быть числом".yellow());
                                continue;
                            }
                        }
                        _ => {
                            println!("{}", format!("Ошибка: неизвестный параметр '{}'", parts[i]).yellow());
                            continue;
                        }
                    }
                }

                match table {
                    "pirates" | "ships" => {
                        println!("{}", format!("Удаляем из таблицы '{}'", table).green());
                        match query.execute(&db).await {
                            Ok(_) => {
                                db.flush_wal_to_bin().await;
                                println!("{}", "Данные успешно удалены!".green());
                            }
                            Err(e) => println!("{}", format!("Ошибка при удалении: {}", e).yellow()),
                        }
                    }
                    _ => println!("{}", format!("Ошибка: неизвестная таблица '{}'. Доступны: pirates, ships", table).yellow()),
                }
            }
            Some("exit") => {
                println!("{}", "Выход...".green());
                break;
            }
            Some(cmd) => println!("{}", format!("Неизвестная команда: {}. Доступны: insert, select, update, delete, exit", cmd).yellow()),
            None => continue,
        }
    }
}
