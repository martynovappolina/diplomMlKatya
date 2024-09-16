"""
create-db
"""

from yoyo import step

__depends__ = {}

steps = [
    step("""
		CREATE TABLE authors (
		id SERIAL PRIMARY KEY, 
		name VARCHAR(100) NOT NULL
		)
	"""),
    step("""
		CREATE TABLE books (
		id SERIAL PRIMARY KEY, 
		title VARCHAR(200) NOT NULL, 
		author_id INTEGER REFERENCES authors(id)
		)
	"""),
    step("""
		CREATE TABLE borrowers (
		id SERIAL PRIMARY KEY, 
		name VARCHAR(100) NOT NULL, 
		email VARCHAR(100) UNIQUE NOT NULL
		)
	"""),
    step("INSERT INTO authors (name) VALUES ('Leo Tolstoy'), ('Fyodor Dostoevsky'), ('Anton Chekhov')"),
    step("INSERT INTO books (title, author_id) VALUES ('War and Peace', 1), ('Crime and Punishment', 2), ('The Chameleon', 3)"),
    step("INSERT INTO borrowers (name, email) VALUES ('Ivan Ivanov', 'ivan@example.com'), ('Maria Petrova', 'maria@example.com')"),
]
