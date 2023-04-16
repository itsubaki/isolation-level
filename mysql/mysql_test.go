package mysql_test

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

func Example_dirtyRead() {
	// create database
	{
		db, err := sql.Open("mysql", "root:secret@(127.0.0.1:3306)/")
		if err != nil {
			panic(err)
		}
		defer db.Close()

		if err := db.Ping(); err != nil {
			panic(err)
		}

		if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS dirty_read"); err != nil {
			panic(err)
		}
	}

	// create table and insert data
	{
		db, err := sql.Open("mysql", "root:secret@(127.0.0.1:3306)/dirty_read")
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

	db, err := sql.Open("mysql", "root:secret@(127.0.0.1:3306)/dirty_read")
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
		rows, err := tx1.Query("SELECT * FROM users")
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
		rows, err := tx1.Query("SELECT * FROM users")
		if err != nil {
			panic(err)
		}
		defer rows.Close()

		// tx2 can see the uncommitted changes.
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

	if err := tx2.Commit(); err != nil {
		panic(err)
	}

	// Output:
	// 1 Alice 100
	// 2 Bob 200
	// 1 Alien 100
	// 2 Bob 200
}

func Example_nonRepeatableRead() {
	// create database
	{
		db, err := sql.Open("mysql", "root:secret@(127.0.0.1:3306)/")
		if err != nil {
			panic(err)
		}
		defer db.Close()

		if err := db.Ping(); err != nil {
			panic(err)
		}

		if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS non_repeatable_read"); err != nil {
			panic(err)
		}
	}

	// create table and insert data
	{
		db, err := sql.Open("mysql", "root:secret@(127.0.0.1:3306)/non_repeatable_read")
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

	db, err := sql.Open("mysql", "root:secret@(127.0.0.1:3306)/non_repeatable_read")
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
		rows, err := tx1.Query("SELECT * FROM users")
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
		// tx1 can't see the committed changes.
		rows, err := tx1.Query("SELECT * FROM users")
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

}
