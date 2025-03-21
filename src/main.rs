use std::io::{self, Write}; // Ввод-вывод — как связь с мостика на астероид!
use tokio; // Асинхронный движок — гиперпространство в деле!
use tokio::time::Duration; // Добавляем Duration для задержек
use yuaidb::{Database, Condition, Query}; // База данных — наш звёздный архив!
use colored::*; // Цвета — голограммы для космической карты!

// Парсим поля вида <field>:<value> — сканируем добычу с орбиты!
fn parse_fields(parts: &[&str]) -> Result<Vec<(String, String)>, String> {
    let mut fields = Vec::new(); // Список добычи — контейнер для звёздного груза!
    let mut i = 0; // Сканер на старте — готов к полёту!

    while i < parts.len() {
        if let Some((field, value_start)) = parts[i].split_once(':') { // Делим сигнал — поле и данные!
            let mut value = String::new(); // Буфер для груза — чистый космос!
            let start_with_quote = value_start.starts_with('"'); // Кавычки? Это шифрованный сигнал!

            if start_with_quote { // Ловим длинный текст — как сообщение с далёкой планеты!
                let mut in_quotes = true; // Лазерный щит активен — ждём закрытия!
                let start = if value_start.len() > 1 { &value_start[1..] } else { "" }; // Отрезаем кавычку — чистим сигнал!
                value.push_str(start); // Первый кусок в трюм!

                i += 1; // Движемся по орбите — дальше в космос!
                while i < parts.len() && in_quotes { // Сканируем до конца шифра!
                    let part = parts[i];
                    if part.ends_with('"') && !part.ends_with("\\\"") { // Конец сигнала — без подвоха!
                        value.push_str(" "); // Пробел — как пустота между звёздами!
                        value.push_str(&part[..part.len() - 1]); // Добавляем чистый кусок!
                        in_quotes = false; // Щит снят — сообщение получено!
                    } else {
                        value.push_str(" "); // Пробел — соединяем обломки!
                        value.push_str(part); // Кидаем в трюм!
                    }
                    i += 1; // Следующий сектор!
                }
                if in_quotes { // Щит не снят? Сигнал обрывается!
                    return Err("Ошибка: незакрытая кавычка в значении — сигнал потерян в гиперпространстве!".to_string());
                }
            } else {
                value.push_str(value_start); // Короткий сигнал — сразу в трюм!
                i += 1; // На следующий сектор!
            }

            fields.push((field.to_string(), value.trim().to_string())); // Добыча в грузовом отсеке — поле и значение!
        } else {
            i += 1; // Пропускаем шум — дальше по курсу!
        }
    }

    if fields.is_empty() { // Трюм пуст? Миссия провалена!
        Err("Ошибка: не найдены поля в формате <field>:<value> — звёздная карта пуста!".to_string())
    } else {
        Ok(fields) // Добыча на борту — полный вперёд!
    }
}

// Парсим условия WHERE — как радар для поиска в туманности!
fn parse_where(parts: &[&str], i: &mut usize, query: &mut Query) -> Result<(), String> {
    let mut current_group = Vec::new(); // Группа условий — как эскадра дронов!

    while *i < parts.len() { // Сканируем космос — ищем цели!
        let part = parts[*i].to_uppercase(); // Сигнал в верхний регистр — чёткость на мостике!
        let is_where_or_logical = part == "WHERE" || part == "OR" || part == "AND"; // Логика или старт — где добыча?

        if is_where_or_logical { // Ловим координаты или логические маяки!
            if part == "OR" && !current_group.is_empty() { // Новый сектор — отделяем эскадру!
                query.where_clauses.push(current_group); // Кидаем в запрос — звёзды выстроены!
                current_group = Vec::new(); // Новый дрон на старте!
            }
            *i += 1; // Движемся по орбите!

            if *i >= parts.len() { // Конец сигнала? Ошибка на радаре!
                return Err(format!("Ошибка: укажите условие после '{}' — радар молчит!", part.to_lowercase()));
            }

            let field = parts[*i]; // Поле — как звезда на карте!
            *i += 1; // Следующий сигнал!

            if *i >= parts.len() { // Нет оператора? Сбой в системе!
                return Err(format!("Ошибка: укажите оператор после поля '{}' — координаты потеряны!", field));
            }

            let operator = parts[*i].to_uppercase(); // Оператор — команда для дрона!
            *i += 1; // Дальше в космос!

            if *i >= parts.len() { // Нет значения? Сигнал обрывается!
                return Err(format!("Ошибка: укажите значение после оператора '{}' — цель не найдена!", operator));
            }

            match operator.as_str() { // Расшифровываем команду!
                "=" => { // Точный выстрел — координаты совпадают!
                    let value = parse_value(parts, i)?; // Ловим сигнал!
                    current_group.push(Condition::Eq(field.to_string(), value)); // Дрон на цель!
                }
                "<" => { // Меньше — фильтруем мелочь в астероидном поясе!
                    let value = parse_value(parts, i)?; // Сигнал пойман!
                    current_group.push(Condition::Lt(field.to_string(), value)); // Дрон в деле!
                }
                ">" => { // Больше — только крупные звёзды!
                    let value = parse_value(parts, i)?; // Цель на радаре!
                    current_group.push(Condition::Gt(field.to_string(), value)); // Лазер наготове!
                }
                "CONTAINS" => { // Ищем тайники в туманности!
                    let value = parse_value(parts, i)?; // Сигнал расшифрован!
                    current_group.push(Condition::Contains(field.to_string(), value)); // Радар сканирует!
                }
                "IN" => { // Проверяем по списку — как патруль в секторе!
                    if !parts[*i].starts_with('(') { // Нет скобки? Ошибка в протоколе!
                        return Err("Ошибка: ожидается '(' после IN — координаты сбиты!".to_string());
                    }
                    let values = parse_in_values(parts, i)?; // Список целей получен!
                    if values.is_empty() { // Пустой список? Миссия провалена!
                        return Err("Ошибка: укажите значения в IN — радар не видит целей!".to_string());
                    }
                    current_group.push(Condition::In(field.to_string(), values)); // Дрон на патруле!
                }
                "BETWEEN" => { // Диапазон — как зона сканирования!
                    let min = parse_value(parts, i)?; // Нижняя граница поймана!
                    if *i >= parts.len() || parts[*i].to_uppercase() != "AND" { // Нет AND? Ошибка в сигнале!
                        return Err("Ошибка: ожидается 'AND' после первого значения BETWEEN — зона не определена!".to_string());
                    }
                    *i += 1; // Следующий сигнал!
                    if *i >= parts.len() { // Нет второго значения? Радар сломан!
                        return Err("Ошибка: укажите второе значение после 'AND' в BETWEEN — зона обрезана!".to_string());
                    }
                    let max = parse_value(parts, i)?; // Верхняя граница на месте!
                    current_group.push(Condition::Between(field.to_string(), min, max)); // Зона сканирования готова!
                }
                _ => return Err(format!("Ошибка: неизвестный оператор '{}' — сигнал с другой галактики!", operator)), // Неизвестная команда? Чужие на связи!
            }
        } else if query.where_clauses.is_empty() && current_group.is_empty() { // Нет WHERE? Радар не настроен!
            return Err(format!("Ошибка: ожидается 'where' в начале условия, найдено '{}' — сбой на мостике!", parts[*i]));
        } else {
            break; // Конец сигнала — выходим из гиперпространства!
        }
    }

    if !current_group.is_empty() { // Остатки эскадры? Кидаем в запрос!
        query.where_clauses.push(current_group); // Дроны на орбите!
    }

    if query.where_clauses.is_empty() { // Радар пуст? Ошибка в системе!
        return Err("Ошибка: условие WHERE не содержит корректных условий — звёзды не найдены!".to_string());
    }

    Ok(()) // Радар настроен — полный вперёд к добыче!
}

// Парсим значение — ловим сигналы с орбиты!
fn parse_value(parts: &[&str], i: &mut usize) -> Result<String, String> {
    let mut value = String::new(); // Буфер для сигнала — чистый космос!

    if parts[*i].starts_with('"') { // Шифрованный сигнал — кавычки в деле!
        let mut in_quotes = true; // Лазерный щит активен!
        let start = &parts[*i][1..]; // Отрезаем кавычку — чистим данные!
        value.push_str(start); // Первый кусок в трюм!

        *i += 1; // Движемся дальше по орбите!
        while *i < parts.len() && in_quotes { // Сканируем до конца!
            let part = parts[*i];
            if part.ends_with('"') && !part.ends_with("\\\"") { // Конец сигнала — чистый выход!
                value.push_str(" "); // Пробел — как вакуум между словами!
                value.push_str(&part[..part.len() - 1]); // Кидаем чистый кусок!
                in_quotes = false; // Щит снят — сигнал получен!
            } else {
                value.push_str(" "); // Соединяем обломки!
                value.push_str(part); // Добавляем в буфер!
            }
            *i += 1; // Следующий сектор!
        }
        if in_quotes { // Щит не снят? Ошибка в эфире!
            return Err("Ошибка: незакрытая кавычка в значении — сигнал потерян в чёрной дыре!".to_string());
        }
    } else {
        value = parts[*i].to_string(); // Простой сигнал — сразу в трюм!
        *i += 1; // На следующий сектор!
    }

    Ok(value.trim().to_string()) // Сигнал пойман — чистый и готовый!
}

// Парсим значения для IN — как список координат в секторе!
fn parse_in_values(parts: &[&str], i: &mut usize) -> Result<Vec<String>, String> {
    let mut values = Vec::new(); // Список целей — координаты для дронов!
    let mut j = *i + 1; // Начинаем с первого значения!

    while j < parts.len() && !parts[j].ends_with(')') { // Сканируем до закрытия!
        if !parts[j].is_empty() && parts[j] != "," { // Пропускаем шум — только данные!
            values.push(parts[j].trim_matches('"').to_string()); // Кидаем координату в список!
        }
        j += 1; // Следующий сигнал!
    }
    if j < parts.len() && parts[j].ends_with(')') { // Закрывающая скобка? Завершаем!
        let last = parts[j][..parts[j].len() - 1].trim_matches('"'); // Последний кусок — чистим!
        if !last.is_empty() { // Не пусто? В трюм!
            values.push(last.to_string());
        }
        *i = j + 1; // Обновляем курсор — сектор пройден!
    } else {
        return Err("Ошибка: незакрытая скобка в IN — координаты потеряны в гиперпространстве!".to_string()); // Нет конца? Сбой!
    }

    Ok(values) // Координаты собраны — дроны готовы!
}

#[tokio::main]
async fn main() {
    // Создаём базу — наш космический корабль с архивом!
    let db = match Database::new("./data", "./config.toml").await {
        Ok(db) => db, // Корабль на орбите — готов к рейду!
        Err(e) => {
            println!("{}", format!("Йо-хо-хо, корабль не вышел из дока: {}!", e).yellow()); // Сбой на старте — тревога на мостике!
            return;
        }
    };

    // Даём шпиону время на первую инициализацию
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Приветствие с мостика — голограмма для юного пирата!
    println!("{}", "Эй, звёздный корсар! Это твой пульт управления галактической базой!".purple().bold());
    println!("{}", "Вставка: insert pirates name:\"Капитан Джек Воробот Бла Бла Бла\" ship_id:101".purple()); // Грузим добычу в трюм!
    println!("{}", "Поиск: select name from pirates where name contains \"Иван\"".purple()); // Сканируем звёзды!
    println!("{}", "Обновка: update pirates set name:\"Капитан Джек Воробот Новый\" where ship_id = 101".purple()); // Чиним дроидов!
    println!("{}", "Чистка: delete from pirates where name = \"Капитан Джек Воробот Бла Бла Бла\"".purple()); // Выкидываем мусор в чёрную дыру!
    println!("{}", "- exit (сматываемся с орбиты)".purple()); // Пора в гиперпространство!

    // Грузим экипаж — пираты на борт!
    let mut query = db.insert("pirates"); // Новый трюм для корсаров!
    query.values(vec![
        vec![("id", "1"), ("name", "Капитан Джек Воробот"), ("ship_id", "101")], // Первый капитан на мостике!
        vec![("id", "2"), ("name", "Лихой Иван"), ("ship_id", "102")], // Второй в деле — штурман!
        vec![("id", "3"), ("name", "Морской Волк"), ("ship_id", "101")], // Третий — стрелок!
    ]);
    if let Err(e) = query.execute(&db).await { // Пробуем поднять экипаж!
        println!("{}", format!("Ошибка при загрузке пиратов в трюм: {}!", e).yellow()); // Сбой в ангаре!
    } else {
        println!("{}", "Пираты на борту — экипаж готов!".green()); // Команда в строю!
    }

    // Спускаем корабли — флот в космос!
    let mut query = db.insert("ships"); // Новый ангар для звездолётов!
    query.values(vec![
        vec![("ship_id", "101"), ("name", "Чёрная Комета"), ("speed", "0.9")], // Быстрый крейсер!
        vec![("ship_id", "102"), ("name", "Астероидный Шторм"), ("speed", "0.7")], // Тяжёлый разрушитель!
    ]);
    if let Err(e) = query.execute(&db).await { // Пробуем запустить флот!
        println!("{}", format!("Ошибка при спуске кораблей в космос: {}!", e).yellow()); // Сбой в доке!
    } else {
        println!("{}", "Флот вышел на орбиту — полный вперёд!".green()); // Звёзды ждут!
    }

    loop { // Главный цикл — мостик в деле!
        print!("{}", "> ".yellow()); // Сигнал с мостика — ждём команду!
        io::stdout().flush().unwrap(); // Очищаем эфир — связь чистая!

        let mut input = String::new(); // Буфер для приказа — готов к декодированию!
        if io::stdin().read_line(&mut input).is_err() { // Ловим сигнал от капитана!
            println!("{}", "Ошибка чтения приказа! Интерференция с астероидов?".yellow()); // Сбой в эфире!
            continue;
        }
        let input = input.trim(); // Чистим шум — только суть!
        if input.is_empty() { // Пустой сигнал? Ждём дальше!
            continue;
        }

        let parts: Vec<&str> = input.split_whitespace().collect(); // Разбиваем приказ на куски — как метеоритный дождь!

        match parts.get(0).map(|s| s.to_lowercase()).as_deref() { // Декодируем первую команду!
            Some("insert") => { // Грузим добычу в трюм!
                if parts.len() < 3 { // Сигнал короткий? Ошибка в протоколе!
                    println!("{}", "Ошибка: укажите ангар и хотя бы одно поле (например, insert pirates name:\"Джек\")".yellow());
                    continue;
                }

                let table = parts[1]; // Ангар для добычи — где прячем?
                match parse_fields(&parts[2..]) { // Сканируем груз!
                    Ok(fields) => {
                        let fields_ref: Vec<(&str, &str)> = fields.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect(); // Преобразуем в сигнал для дроидов!
                        let mut query = db.insert(table); // Новый запрос — ангар готов!
                        query.values(fields_ref.clone()); // Грузим добычу!
                        
                        // Новый комментарий: Проверяем наличие ангара в базе динамически
                        let available_tables: Vec<String> = db.tables.iter().map(|t| t.key().clone()).collect();
                        if db.tables.contains_key(table) { // Ангар на карте!
                            println!("{}", format!("Грузим добычу в ангар '{}': {:?}", table, fields).green()); // Сигнал на мостик!
                            if let Err(e) = query.execute(&db).await { // Пробуем спрятать груз!
                                println!("{}", format!("Космический шторм помешал: {}!", e).yellow()); // Сбой в гиперпространстве!
                            } else {
                                println!("{}", "Добыча в ангаре — полный порядок!".green()); // Успех — звёзды наши!
                            }
                        } else {
                            println!("{}", format!("Ошибка: неизвестный ангар '{}'. Доступны: {}", table, available_tables.join(", ")).yellow()); // Чужой сектор!
                        }
                    }
                    Err(e) => println!("{}", format!("Ошибка в звёздной карте добычи: {}!", e).yellow()), // Карта повреждена!
                }
            }
            Some("select") => { // Сканируем космос — ищем добычу!
                if parts.len() < 4 { // Сигнал слабый? Ошибка в протоколе!
                    println!("{}", "Ошибка: неверный формат. Используйте: select <fields> from <table> [where ...]".yellow());
                    continue;
                }

                let from_idx = parts.iter().position(|&p| p.to_lowercase() == "from"); // Ищем маяк FROM!
                if from_idx.is_none() || from_idx.unwrap() < 2 { // Нет маяка? Сбой на радаре!
                    println!("{}", "Ошибка: укажите 'from' после полей — координаты потеряны!".yellow());
                    continue;
                }
                let from_idx = from_idx.unwrap(); // Маяк пойман!

                let fields_str = parts[1..from_idx].join(" "); // Собираем поля — как звёзды в созвездии!
                let fields = fields_str.split(',').map(|s| s.trim()).collect::<Vec<&str>>(); // Разделяем сигналы!
                let table = parts[from_idx + 1]; // Ангар для поиска!
                let mut query = db.select(table); // Новый запрос — радар включён!
                query.fields(fields); // Настраиваем сканер!
                let mut i = from_idx + 2; // Курсор на орбите!

                while i < parts.len() { // Сканируем дальше — что ещё на карте?
                    match parts[i].to_lowercase().as_str() { // Декодируем сигнал!
                        "as" => { // Псевдоним — кличка для ангара!
                            if i + 1 >= parts.len() { // Нет клички? Ошибка в эфире!
                                println!("{}", "Ошибка: укажите алиас после 'as' — сигнал обрывается!".yellow());
                                break;
                            }
                            query.alias(parts[i + 1]); // Даём кличку!
                            i += 2; // Следующий сектор!
                        }
                        "join" => { // Связываем флот — как эскадру в бою!
                            if i + 5 >= parts.len() || parts[i + 2].to_lowercase() != "as" || parts[i + 4].to_lowercase() != "on" { // Формат сбит?
                                println!("{}", "Ошибка: неверный формат JOIN. Используйте: join <table> as <alias> on <on_left> = <on_right>".yellow());
                                break;
                            }
                            let join_table = parts[i + 1]; // Новый ангар!
                            let join_alias = parts[i + 3]; // Его кличка!
                            let on_left = parts[i + 5]; // Левая координата!
                            if i + 6 >= parts.len() || parts[i + 6] != "=" { // Нет равенства? Ошибка!
                                println!("{}", "Ошибка: укажите условие JOIN в формате <on_left> = <on_right>".yellow());
                                break;
                            }
                            let on_right = parts[i + 7]; // Правая координата!
                            query.join(join_table, join_alias, on_left, on_right); // Связываем флот!
                            i += 8; // Прыгаем дальше!
                        }
                        "where" => { // Фильтр — радар в деле!
                            if let Err(e) = parse_where(&parts, &mut i, &mut query) { // Сбой в настройке?
                                println!("{}", format!("Ошибка в настройке радара: {}!", e).yellow());
                                break;
                            }
                        }
                        "order" => { // Сортировка — порядок в эскадре!
                            if i + 1 >= parts.len() || parts[i + 1].to_lowercase() != "by" { // Нет BY? Сбой!
                                println!("{}", "Ошибка: укажите 'by' после 'order' — порядок потерян!".yellow());
                                break;
                            }
                            if i + 2 >= parts.len() { // Нет поля? Ошибка!
                                println!("{}", "Ошибка: укажите поле после 'order by' — звёзды в хаосе!".yellow());
                                break;
                            }
                            let order_field = parts[i + 2]; // Поле для порядка!
                            let ascending = if i + 3 < parts.len() { // Проверяем направление!
                                match parts[i + 3].to_lowercase().as_str() {
                                    "asc" => true, // Вверх по орбите!
                                    "desc" => false, // Вниз к чёрной дыре!
                                    _ => true, // По умолчанию вверх!
                                }
                            } else {
                                true // Вверх, если не указано!
                            };
                            query.order_by(order_field, ascending); // Настраиваем порядок!
                            i += if i + 3 < parts.len() && (parts[i + 3].to_lowercase() == "asc" || parts[i + 3].to_lowercase() == "desc") { 4 } else { 3 }; // Прыгаем дальше!
                        }
                        "limit" => { // Лимит — сколько звёзд утащить!
                            if i + 1 >= parts.len() { // Нет числа? Ошибка!
                                println!("{}", "Ошибка: укажите число после 'limit' — сколько добычи брать?".yellow());
                                break;
                            }
                            if let Ok(limit) = parts[i + 1].parse::<usize>() { // Число поймано!
                                query.limit(limit); // Устанавливаем лимит!
                                i += 2; // Следующий сектор!
                            } else {
                                println!("{}", "Ошибка: 'limit' должен быть числом — сбой в вычислениях!".yellow());
                                break;
                            }
                        }
                        "offset" => { // Смещение — пропускаем первые звёзды!
                            if i + 1 >= parts.len() { // Нет числа? Ошибка!
                                println!("{}", "Ошибка: укажите число после 'offset' — с какой звезды начинать?".yellow());
                                break;
                            }
                            if let Ok(offset) = parts[i + 1].parse::<usize>() { // Число на радаре!
                                query.offset(offset); // Устанавливаем смещение!
                                i += 2; // Дальше по курсу!
                            } else {
                                println!("{}", "Ошибка: 'offset' должен быть числом — сбой в координатах!".yellow());
                                break;
                            }
                        }
                        _ => { // Неизвестный сигнал? Чужие на связи!
                            println!("{}", format!("Ошибка: неизвестный маяк на карте '{}' — что за галактика?", parts[i]).yellow());
                            break;
                        }
                    }
                }

                match query.execute(&db).await { // Запускаем сканер!
                    Ok(Some(rows)) => { // Добыча найдена!
                        for row in rows { // Показываем улов!
                            println!("{}", format!("Добыча с орбиты: {:?}", row).green()); // Сигнал на мостик!
                        }
                    }
                    Ok(None) => println!("{}", "Ангар пуст, звёздный корсар!".green()), // Ничего нет — тишина в космосе!
                    Err(e) => println!("{}", format!("Космический шторм помешал: {}!", e).yellow()), // Сбой в гиперпространстве!
                }
            }
            Some("update") => { // Чиним добычу — обновка в ангаре!
                if parts.len() < 4 || parts[2].to_lowercase() != "set" { // Сигнал сбит? Ошибка!
                    println!("{}", "Ошибка: неверный формат. Используйте: update <table> set <field1>:<value1> ... [where ...]".yellow());
                    continue;
                }

                let table = parts[1]; // Ангар для ремонта!
                let mut i = 3; // Курсор на старте!
                let fields_end = parts[i..].iter().position(|&p| p.to_lowercase() == "where").unwrap_or(parts.len() - i); // Ищем WHERE!
                match parse_fields(&parts[i..i + fields_end]) { // Сканируем новый груз!
                    Ok(fields) => {
                        let fields_ref: Vec<(&str, &str)> = fields.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect(); // Готовим сигнал для дроидов!
                        let mut query = db.update(table); // Новый запрос — ремонтный ангар!
                        query.values(fields_ref.clone()); // Кидаем новый груз!
                        i += fields_end; // Прыгаем дальше!

                        if i >= parts.len() { // Нет WHERE? Ошибка в протоколе!
                            println!("{}", "Ошибка: для команды update требуется условие WHERE — где чинить?".yellow());
                            continue;
                        }

                        if parts[i].to_lowercase() == "where" { // Фильтр для ремонта!
                            if let Err(e) = parse_where(&parts, &mut i, &mut query) { // Сбой в настройке?
                                println!("{}", format!("Ошибка в настройке радара: {}!", e).yellow());
                                continue;
                            }
                        } else { // Нет WHERE? Сбой!
                            println!("{}", format!("Ошибка: ожидается 'where', найдено '{}' — координаты потеряны!", parts[i]).yellow());
                            continue;
                        }

                        // Новый комментарий: Проверяем наличие ангара в базе динамически
                        let available_tables: Vec<String> = db.tables.iter().map(|t| t.key().clone()).collect();
                        if db.tables.contains_key(table) { // Ангар на карте!
                            println!("{}", format!("Обновляем добычу в ангаре '{}': {:?}", table, fields).green()); // Сигнал на мостик!
                            if let Err(e) = query.execute(&db).await { // Пробуем чинить!
                                println!("{}", format!("Ошибка при обновлении добычи: {}!", e).yellow()); // Сбой в ангаре!
                            } else {
                                println!("{}", "Добыча обновлена — ангар в порядке!".green()); // Успех — звёзды сияют!
                            }
                        } else {
                            println!("{}", format!("Ошибка: неизвестный ангар '{}'. Доступны: {}", table, available_tables.join(", ")).yellow()); // Чужой сектор!
                        }
                    }
                    Err(e) => println!("{}", format!("Ошибка в звёздной карте добычи: {}!", e).yellow()), // Карта повреждена!
                }
            }
            Some("delete") => { // Чистим ангар — мусор в чёрную дыру!
                if parts.len() < 3 || parts[1].to_lowercase() != "from" { // Сигнал сбит? Ошибка!
                    println!("{}", "Ошибка: неверный формат. Используйте: delete from <table> [where ...]".yellow());
                    continue;
                }

                let table = parts[2]; // Ангар для чистки!
                let mut query = db.delete(table); // Новый запрос — дроны на выброс!
                let mut i = 3; // Курсор на орбите!

                if i >= parts.len() { // Нет WHERE? Ошибка в протоколе!
                    println!("{}", "Ошибка: для команды delete требуется условие WHERE — что выкидывать?".yellow());
                    continue;
                }

                if parts[i].to_lowercase() == "where" { // Фильтр для чистки!
                    if let Err(e) = parse_where(&parts, &mut i, &mut query) { // Сбой в настройке?
                        println!("{}", format!("Ошибка в настройке радара: {}!", e).yellow());
                        continue;
                    }
                } else { // Нет WHERE? Сбой!
                    println!("{}", format!("Ошибка: ожидается 'where', найдено '{}' — координаты потеряны!", parts[i]).yellow());
                    continue;
                }

                while i < parts.len() { // Сканируем дальше — дополнительные команды!
                    match parts[i].to_lowercase().as_str() { // Декодируем сигнал!
                        "limit" => { // Лимит — сколько мусора выкинуть!
                            if i + 1 >= parts.len() { // Нет числа? Ошибка!
                                println!("{}", "Ошибка: укажите число после 'limit' — сколько выбрасывать?".yellow());
                                continue;
                            }
                            if let Ok(limit) = parts[i + 1].parse::<usize>() { // Число поймано!
                                query.limit(limit); // Устанавливаем лимит!
                                i += 2; // Следующий сектор!
                            } else {
                                println!("{}", "Ошибка: 'limit' должен быть числом — сбой в вычислениях!".yellow());
                                continue;
                            }
                        }
                        "offset" => { // Смещение — пропускаем первые куски!
                            if i + 1 >= parts.len() { // Нет числа? Ошибка!
                                println!("{}", "Ошибка: укажите число после 'offset' — с чего начинать?".yellow());
                                continue;
                            }
                            if let Ok(offset) = parts[i + 1].parse::<usize>() { // Число на радаре!
                                query.offset(offset); // Устанавливаем смещение!
                                i += 2; // Дальше по курсу!
                            } else {
                                println!("{}", "Ошибка: 'offset' должен быть числом — сбой в координатах!".yellow());
                                continue;
                            }
                        }
                        _ => { // Неизвестный сигнал? Чужие на связи!
                            println!("{}", format!("Ошибка: неизвестный маяк на карте '{}' — что за галактика?", parts[i]).yellow());
                            continue;
                        }
                    }
                }

                // Проверяем наличие ангара в базе динамически
                let available_tables: Vec<String> = db.tables.iter().map(|t| t.key().clone()).collect();
                if db.tables.contains_key(table) { // Ангар на карте!
                    println!("{}", format!("Выкидываем мусор из ангара '{}'", table).green()); // Сигнал на мостик!
                    if let Err(e) = query.execute(&db).await { // Пробуем чистить!
                        println!("{}", format!("Ошибка при выбросе в чёрную дыру: {}!", e).yellow()); // Сбой в ангаре!
                    } else {
                        println!("{}", "Мусор в космосе — ангар чист!".green()); // Успех — порядок на орбите!
                    }
                } else {
                    println!("{}", format!("Ошибка: неизвестный ангар '{}'. Доступны: {}", table, available_tables.join(", ")).yellow()); // Чужой сектор!
                }
            }
            Some("exit") => { // Сматываемся с орбиты!
                println!("{}", "До новых звёздных рейдов, корсар!".green()); // Прощальный сигнал!
                break; // Прыжок в гиперпространство!
            }
            Some(cmd) => println!("{}", format!("Неизвестный сигнал: {}. Доступны: insert, select, update, delete, exit", cmd).yellow()), // Чужая команда!
            None => continue, // Пустой эфир — ждём дальше!
        }
    }
}
