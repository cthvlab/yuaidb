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
