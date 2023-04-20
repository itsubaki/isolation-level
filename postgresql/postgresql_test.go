package postgresql_test

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

// https://www.postgresql.org/docs/current/transaction-iso.html
// Table 13.1. Transaction Isolation Levels
//
// | Isolation Level  | Dirty Read             | Nonrepeatable Read | Phantom Read           | Serialization Anomaly |
// |------------------|------------------------|--------------------|------------------------|-----------------------|
// | Read uncommitted | Allowed, but not in PG | Possible           | Possible               | Possible              |
// | Read committed   | Not possible           | Possible           | Possible               | Possible              |
// | Repeatable read  | Not possible           | Not possible       | Allowed, but not in PG | Possible              |
// | Serializable     | Not possible           | Not possible       | Not possible           | Not possible          |

func Example_dirtyRead_not() {
	// create database
	{
		db, err := sql.Open("postgres", "host=127.0.0.1 port=5432 user=postgres password=secret sslmode=disable")
		if err != nil {
			panic(err)
		}
		defer db.Close()

		if err := db.Ping(); err != nil {
			panic(err)
		}

		if _, err := db.Exec("DROP DATABASE IF EXISTS dirty_read"); err != nil {
			panic(err)
		}

		if _, err := db.Exec("CREATE DATABASE dirty_read"); err != nil {
			panic(err)
		}
	}

	// create table and insert data
	{
		db, err := sql.Open("postgres", "host=127.0.0.1 port=5432 user=postgres password=secret dbname=dirty_read sslmode=disable")
		if err != nil {
			panic(err)
		}
		defer db.Close()

		if err := db.Ping(); err != nil {
			panic(err)
		}

		if _, err := db.Exec("CREATE TABLE IF NOT EXISTS users (id INT, name VARCHAR(255), score INT)"); err != nil {
			panic(err)
		}

		if _, err := db.Exec("DELETE FROM users"); err != nil {
			panic(err)
		}

		if _, err := db.Exec("INSERT INTO users (id, name, score) VALUES (1, 'Alice', 100)"); err != nil {
			panic(err)
		}

		if _, err := db.Exec("INSERT INTO users (id, name, score) VALUES (2, 'Bob', 200)"); err != nil {
			panic(err)
		}
	}

	db, err := sql.Open("postgres", "host=127.0.0.1 port=5432 user=postgres password=secret dbname=dirty_read sslmode=disable")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	tx1, err := db.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelReadUncommitted,
	})
	if err != nil {
		panic(err)
	}

	tx2, err := db.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelReadUncommitted,
	})
	if err != nil {
		panic(err)
	}

	{
		rows, err := tx1.Query("SELECT * FROM users ORDER BY id")
		if err != nil {
			panic(err)
		}
		defer rows.Close()

		// 1 Alice 100
		// 2 Bob 200
		for rows.Next() {
			var id int
			var name string
			var score int
			if err := rows.Scan(&id, &name, &score); err != nil {
				panic(err)
			}

			fmt.Println(id, name, score)
		}
	}

	// Update but not commit. Alice -> Alien.
	if _, err := tx2.Exec("UPDATE users SET name = 'Alien' WHERE id = 1"); err != nil {
		panic(err)
	}

	{
		rows, err := tx1.Query("SELECT * FROM users ORDER BY id")
		if err != nil {
			panic(err)
		}
		defer rows.Close()

		// tx1 can't see the uncommitted changes.
		// 1 Alice 100
		// 2 Bob 200
		for rows.Next() {
			var id int
			var name string
			var score int
			if err := rows.Scan(&id, &name, &score); err != nil {
				panic(err)
			}

			fmt.Println(id, name, score)
		}
	}

	if err := tx1.Commit(); err != nil {
		panic(err)
	}

	if err := tx2.Commit(); err != nil {
		panic(err)
	}

	// Output:
	// 1 Alice 100
	// 2 Bob 200
	// 1 Alice 100
	// 2 Bob 200
}

func Example_nonRepeatableRead() {
	// create database
	{
		db, err := sql.Open("postgres", "host=127.0.0.1 port=5432 user=postgres password=secret sslmode=disable")
		if err != nil {
			panic(err)
		}
		defer db.Close()

		if err := db.Ping(); err != nil {
			panic(err)
		}

		if _, err := db.Exec("DROP DATABASE IF EXISTS non_repeatable_read"); err != nil {
			panic(err)
		}

		if _, err := db.Exec("CREATE DATABASE non_repeatable_read"); err != nil {
			panic(err)
		}
	}

	// create table and insert data
	{
		db, err := sql.Open("postgres", "host=127.0.0.1 port=5432 user=postgres password=secret dbname=non_repeatable_read sslmode=disable")
		if err != nil {
			panic(err)
		}
		defer db.Close()

		if err := db.Ping(); err != nil {
			panic(err)
		}

		if _, err := db.Exec("CREATE TABLE IF NOT EXISTS users (id INT, name VARCHAR(255), score INT)"); err != nil {
			panic(err)
		}

		if _, err := db.Exec("DELETE FROM users"); err != nil {
			panic(err)
		}

		if _, err := db.Exec("INSERT INTO users (id, name, score) VALUES (1, 'Alice', 100)"); err != nil {
			panic(err)
		}

		if _, err := db.Exec("INSERT INTO users (id, name, score) VALUES (2, 'Bob', 200)"); err != nil {
			panic(err)
		}
	}

	db, err := sql.Open("postgres", "host=127.0.0.1 port=5432 user=postgres password=secret dbname=non_repeatable_read sslmode=disable")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	tx1, err := db.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	if err != nil {
		panic(err)
	}

	tx2, err := db.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	if err != nil {
		panic(err)
	}

	{
		rows, err := tx1.Query("SELECT * FROM users ORDER BY id")
		if err != nil {
			panic(err)
		}
		defer rows.Close()

		// 1 Alce 100
		// 2 Bob 200
		for rows.Next() {
			var id int
			var name string
			var score int
			if err := rows.Scan(&id, &name, &score); err != nil {
				panic(err)
			}

			fmt.Println(id, name, score)
		}
	}

	// Update and commit. Alice -> Alien.
	if _, err := tx2.Exec("UPDATE users SET name = 'Alien' WHERE id = 1"); err != nil {
		panic(err)
	}

	if err := tx2.Commit(); err != nil {
		panic(err)
	}

	{
		// tx1 see a different result than before
		rows, err := tx1.Query("SELECT * FROM users ORDER BY id")
		if err != nil {
			panic(err)
		}
		defer rows.Close()

		// 1 Alien 100
		// 2 Bob 200
		for rows.Next() {
			var id int
			var name string
			var score int
			if err := rows.Scan(&id, &name, &score); err != nil {
				panic(err)
			}

			fmt.Println(id, name, score)
		}
	}

	if err := tx1.Commit(); err != nil {
		panic(err)
	}

	// Output:
	// 1 Alice 100
	// 2 Bob 200
	// 1 Alien 100
	// 2 Bob 200
}

func Example_phantomRead() {
	// create database
	{
		db, err := sql.Open("postgres", "host=127.0.0.1 port=5432 user=postgres password=secret sslmode=disable")
		if err != nil {
			panic(err)
		}
		defer db.Close()

		if err := db.Ping(); err != nil {
			panic(err)
		}

		if _, err := db.Exec("DROP DATABASE IF EXISTS phantom_read"); err != nil {
			panic(err)
		}

		if _, err := db.Exec("CREATE DATABASE phantom_read"); err != nil {
			panic(err)
		}
	}

	// create table and insert data
	{
		db, err := sql.Open("postgres", "host=127.0.0.1 port=5432 user=postgres password=secret dbname=phantom_read sslmode=disable")
		if err != nil {
			panic(err)
		}
		defer db.Close()

		if err := db.Ping(); err != nil {
			panic(err)
		}

		if _, err := db.Exec("CREATE TABLE IF NOT EXISTS users (id INT, name VARCHAR(255), score INT)"); err != nil {
			panic(err)
		}

		if _, err := db.Exec("DELETE FROM users"); err != nil {
			panic(err)
		}

		if _, err := db.Exec("INSERT INTO users (id, name, score) VALUES (1, 'Alice', 100)"); err != nil {
			panic(err)
		}

		if _, err := db.Exec("INSERT INTO users (id, name, score) VALUES (2, 'Bob', 200)"); err != nil {
			panic(err)
		}
	}

	db, err := sql.Open("postgres", "host=127.0.0.1 port=5432 user=postgres password=secret dbname=phantom_read sslmode=disable")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	tx1, err := db.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	if err != nil {
		panic(err)
	}

	tx2, err := db.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	if err != nil {
		panic(err)
	}

	{
		rows, err := tx1.Query("SELECT * FROM users ORDER BY id")
		if err != nil {
			panic(err)
		}
		defer rows.Close()

		// 1 Alce 100
		// 2 Bob 200
		for rows.Next() {
			var id int
			var name string
			var score int
			if err := rows.Scan(&id, &name, &score); err != nil {
				panic(err)
			}

			fmt.Println(id, name, score)
		}
	}

	// Insert and commit.
	if _, err := tx2.Exec("INSERT INTO users (id, name, score) VALUES (3, 'Charlie', 300)"); err != nil {
		panic(err)
	}

	if err := tx2.Commit(); err != nil {
		panic(err)
	}

	{
		rows, err := tx1.Query("SELECT * FROM users ORDER BY id")
		if err != nil {
			panic(err)
		}
		defer rows.Close()

		// tx1 see a different result than before
		// 1 Alce 100
		// 2 Bob 200
		// 3 Charlie 300
		for rows.Next() {
			var id int
			var name string
			var score int
			if err := rows.Scan(&id, &name, &score); err != nil {
				panic(err)
			}

			fmt.Println(id, name, score)
		}
	}

	if err := tx1.Commit(); err != nil {
		panic(err)
	}

	// Output:
	// 1 Alice 100
	// 2 Bob 200
	// 1 Alice 100
	// 2 Bob 200
	// 3 Charlie 300
}
