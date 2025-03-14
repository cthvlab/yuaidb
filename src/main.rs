use std::io::{self, Write}; // Ввод-вывод — наше окно в мир!
use tokio; // Асинхронность — время летит быстро!
use yuaidb::Database; // Наша база — сердце программы!

#[tokio::main]
async fn main() {
    // Запускаем базу — готовим пиратский склад!
    let db = Database::new("./data", "./config.toml").await;
	// Инструкции — без паники!
	    println!("Интерактивное добавление данных в базу. Доступные команды:");
	    println!("- insert <table> <field1:value1> ... (например, insert pirates name:Джек ship_id:101)");
	    println!("- select <fields> from <table> [as <alias>] [join ...]"); 
	    println!("- exit (для выхода)");

    // Пакетная вставка — грузим пиратов оптом!
    db.insert("pirates")
        .values(vec![
            vec![("id", "1"), ("name", "Капитан Джек Воробот"), ("ship_id", "101")],
            vec![("id", "2"), ("name", "Лихой Билл"), ("ship_id", "102")],
        ])
        .execute(&db)
        .await;

    // Добавляем корабли — флот готов к бою!
    db.insert("ships")
        .values(vec![
            vec![("ship_id", "101"), ("name", "Чёрная Комета"), ("speed", "0.9c")],
            vec![("ship_id", "102"), ("name", "Астероидный Шторм"), ("speed", "0.7c")],
        ])
        .execute(&db)
        .await;

    // Цикл команд — слушаем капитана!
    loop {
        print!("> ");
        io::stdout().flush().unwrap(); // Промываем терминал — чистота!

        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap(); // Читаем приказ!
        let input = input.trim();

        if input.is_empty() { continue; } // Пусто? Ждём дальше!

        let parts: Vec<&str> = input.split_whitespace().collect();
        match parts[0] {
            "insert" => {
                if parts.len() < 3 { // Мало данных? Ошибка!
                    println!("Ошибка: укажите таблицу и поле (например, insert pirates name:Джек)");
                    continue;
                }

                let table = parts[1];
                // Парсим поля — выуживаем пары!
                let fields: Vec<(&str, &str)> = parts[2..].iter().filter_map(|part| part.split_once(':')).collect();

                if fields.is_empty() { // Нет пар? Плохо!
                    println!("Ошибка: неверный формат. Используйте <field>:<value>");
                    continue;
                }

                match table {
                    "pirates" | "ships" => { // Знакомая таблица? Вперёд!
                        println!("Добавляем в '{}': {:?}", table, fields);
                        db.insert(table).values(vec![fields]).execute(&db).await; // Вставляем — готово!
                        println!("Данные успешно добавлены!");
                    }
                    _ => println!("Ошибка: неизвестная таблица '{}'", table), // Чужак? Не пускаем!
                }
            }
            "select" => {
                if parts.len() < 4 || parts[2] != "from" { // Формат сломан? Ошибка!
                    println!("Ошибка: используйте: select <fields> from <table> ...");
                    continue;
                }

                let fields = parts[1].split(',').map(|s| s.trim()).collect::<Vec<&str>>(); // Поля — в список!
                let table = parts[3];
                let mut query = db.select(table).fields(fields); // Запрос — старт!

                let mut i = 4;
                if i < parts.len() && parts[i] == "as" { // Алиас? Добавляем!
                    if i + 1 >= parts.len() { println!("Ошибка: укажите алиас после 'as'"); continue; }
                    query = query.alias(parts[i + 1]);
                    i += 2;
                }

                if i < parts.len() && parts[i] == "join" { // Джоин? Связываем!
                    if i + 5 >= parts.len() || parts[i + 2] != "as" || parts[i + 4] != "on" {
                        println!("Ошибка: JOIN — join <table> as <alias> on <left> = <right>");
                        continue;
                    }
                    let join_table = parts[i + 1];
                    let join_alias = parts[i + 3];
                    let on_left = parts[i + 5];
                    if i + 6 >= parts.len() || parts[i + 6] != "=" { println!("Ошибка: JOIN требует ="); continue; }
                    let on_right = parts[i + 7];
                    query = query.join(join_table, join_alias, on_left, on_right);
                    i += 8;
                }

                if i < parts.len() && parts[i] == "where" { // Условие? Фильтруем!
                    if i + 3 >= parts.len() || parts[i + 2] != "=" {
                        println!("Ошибка: WHERE — where <field> = <value>");
                        continue;
                    }
                    query = query.where_eq(parts[i + 1], parts[i + 3]);
                }

                // Выполняем — показываем добычу!
                if let Some(rows) = query.execute(&db).await {
                    for row in rows { println!("{:?}", row); }
                } else {
                    println!("Нет данных — пустой сундук!");
                }
            }
            "exit" => { // Выход? Прощаемся!
                println!("Выход...");
                break;
            }
            _ => println!("Неизвестная команда: {}. Есть: insert, select, exit", parts[0]), // Чужое? Не знаем!
        }
    }

    // Финальный показ — пираты и их корабли!
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
