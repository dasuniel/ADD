-- ============================================
-- SAKILA DATABASE SCHEMA
-- DVD Rental Store Chain Database
-- ============================================

-- Tabla: language (Idiomas disponibles)
CREATE TABLE IF NOT EXISTS language (
    language_id SERIAL PRIMARY KEY,
    name CHAR(20) NOT NULL,
    last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Tabla: category (Categorías de películas)
CREATE TABLE IF NOT EXISTS category (
    category_id SERIAL PRIMARY KEY,
    name VARCHAR(25) NOT NULL,
    last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Tabla: country (Países)
CREATE TABLE IF NOT EXISTS country (
    country_id SERIAL PRIMARY KEY,
    country VARCHAR(50) NOT NULL,
    last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Tabla: city (Ciudades)
CREATE TABLE IF NOT EXISTS city (
    city_id SERIAL PRIMARY KEY,
    city VARCHAR(50) NOT NULL,
    country_id SMALLINT NOT NULL,
    last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_city_country FOREIGN KEY (country_id) 
        REFERENCES country (country_id) ON DELETE RESTRICT ON UPDATE CASCADE
);

-- Tabla: address (Direcciones)
CREATE TABLE IF NOT EXISTS address (
    address_id SERIAL PRIMARY KEY,
    address VARCHAR(50) NOT NULL,
    address2 VARCHAR(50),
    district VARCHAR(20) NOT NULL,
    city_id SMALLINT NOT NULL,
    postal_code VARCHAR(10),
    phone VARCHAR(20) NOT NULL,
    last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_address_city FOREIGN KEY (city_id) 
        REFERENCES city (city_id) ON DELETE RESTRICT ON UPDATE CASCADE
);

-- Tabla: store (Tiendas)
CREATE TABLE IF NOT EXISTS store (
    store_id SERIAL PRIMARY KEY,
    manager_staff_id SMALLINT NOT NULL,
    address_id SMALLINT NOT NULL,
    last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_store_address FOREIGN KEY (address_id) 
        REFERENCES address (address_id) ON DELETE RESTRICT ON UPDATE CASCADE
);

-- Tabla: customer (Clientes)
CREATE TABLE IF NOT EXISTS customer (
    customer_id SERIAL PRIMARY KEY,
    store_id SMALLINT NOT NULL,
    first_name VARCHAR(45) NOT NULL,
    last_name VARCHAR(45) NOT NULL,
    email VARCHAR(50),
    address_id SMALLINT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    create_date DATE NOT NULL DEFAULT CURRENT_DATE,
    last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_customer_address FOREIGN KEY (address_id) 
        REFERENCES address (address_id) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_customer_store FOREIGN KEY (store_id) 
        REFERENCES store (store_id) ON DELETE RESTRICT ON UPDATE CASCADE
);

-- Tabla: staff (Personal/Empleados)
CREATE TABLE IF NOT EXISTS staff (
    staff_id SERIAL PRIMARY KEY,
    first_name VARCHAR(45) NOT NULL,
    last_name VARCHAR(45) NOT NULL,
    address_id SMALLINT NOT NULL,
    picture BYTEA,
    email VARCHAR(50),
    store_id SMALLINT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    username VARCHAR(16) NOT NULL,
    password VARCHAR(40),
    last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_staff_address FOREIGN KEY (address_id) 
        REFERENCES address (address_id) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_staff_store FOREIGN KEY (store_id) 
        REFERENCES store (store_id) ON DELETE RESTRICT ON UPDATE CASCADE
);

-- Añadir FK de store a staff (relación circular)
ALTER TABLE store ADD CONSTRAINT fk_store_staff 
    FOREIGN KEY (manager_staff_id) 
    REFERENCES staff (staff_id) ON DELETE RESTRICT ON UPDATE CASCADE;

-- Tabla: actor (Actores)
CREATE TABLE IF NOT EXISTS actor (
    actor_id SERIAL PRIMARY KEY,
    first_name VARCHAR(45) NOT NULL,
    last_name VARCHAR(45) NOT NULL,
    last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Tabla: film (Películas)
CREATE TABLE IF NOT EXISTS film (
    film_id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    description TEXT,
    release_year INTEGER,
    language_id SMALLINT NOT NULL,
    original_language_id SMALLINT,
    rental_duration SMALLINT NOT NULL DEFAULT 3,
    rental_rate DECIMAL(4,2) NOT NULL DEFAULT 4.99,
    length SMALLINT,
    replacement_cost DECIMAL(5,2) NOT NULL DEFAULT 19.99,
    rating VARCHAR(10) DEFAULT 'G',
    special_features TEXT,
    last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_film_language FOREIGN KEY (language_id) 
        REFERENCES language (language_id) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_film_language_original FOREIGN KEY (original_language_id) 
        REFERENCES language (language_id) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT CHECK_special_features CHECK(special_features IS NULL OR 
        special_features ~ 'Trailers|Commentaries|Deleted Scenes|Behind the Scenes'),
    CONSTRAINT CHECK_rating CHECK(rating IN ('G','PG','PG-13','R','NC-17'))
);

-- Tabla: film_actor (Relación Películas-Actores)
CREATE TABLE IF NOT EXISTS film_actor (
    actor_id SMALLINT NOT NULL,
    film_id SMALLINT NOT NULL,
    last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (actor_id, film_id),
    CONSTRAINT fk_film_actor_actor FOREIGN KEY (actor_id) 
        REFERENCES actor (actor_id) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_film_actor_film FOREIGN KEY (film_id) 
        REFERENCES film (film_id) ON DELETE RESTRICT ON UPDATE CASCADE
);

-- Tabla: film_category (Relación Películas-Categorías)
CREATE TABLE IF NOT EXISTS film_category (
    film_id SMALLINT NOT NULL,
    category_id SMALLINT NOT NULL,
    last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (film_id, category_id),
    CONSTRAINT fk_film_category_film FOREIGN KEY (film_id) 
        REFERENCES film (film_id) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_film_category_category FOREIGN KEY (category_id) 
        REFERENCES category (category_id) ON DELETE RESTRICT ON UPDATE CASCADE
);

-- Tabla: inventory (Inventario de películas)
CREATE TABLE IF NOT EXISTS inventory (
    inventory_id SERIAL PRIMARY KEY,
    film_id SMALLINT NOT NULL,
    store_id SMALLINT NOT NULL,
    last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_inventory_film FOREIGN KEY (film_id) 
        REFERENCES film (film_id) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_inventory_store FOREIGN KEY (store_id) 
        REFERENCES store (store_id) ON DELETE RESTRICT ON UPDATE CASCADE
);

-- Tabla: rental (Alquileres)
CREATE TABLE IF NOT EXISTS rental (
    rental_id SERIAL PRIMARY KEY,
    rental_date TIMESTAMP NOT NULL,
    inventory_id INTEGER NOT NULL,
    customer_id SMALLINT NOT NULL,
    return_date TIMESTAMP,
    staff_id SMALLINT NOT NULL,
    last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_rental_inventory FOREIGN KEY (inventory_id) 
        REFERENCES inventory (inventory_id) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_rental_customer FOREIGN KEY (customer_id) 
        REFERENCES customer (customer_id) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_rental_staff FOREIGN KEY (staff_id) 
        REFERENCES staff (staff_id) ON DELETE RESTRICT ON UPDATE CASCADE,
    UNIQUE (rental_date, inventory_id, customer_id)
);

-- Tabla: payment (Pagos)
CREATE TABLE IF NOT EXISTS payment (
    payment_id SERIAL PRIMARY KEY,
    customer_id SMALLINT NOT NULL,
    staff_id SMALLINT NOT NULL,
    rental_id INTEGER,
    amount DECIMAL(5,2) NOT NULL,
    payment_date TIMESTAMP NOT NULL,
    last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_payment_customer FOREIGN KEY (customer_id) 
        REFERENCES customer (customer_id) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_payment_staff FOREIGN KEY (staff_id) 
        REFERENCES staff (staff_id) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_payment_rental FOREIGN KEY (rental_id) 
        REFERENCES rental (rental_id) ON DELETE SET NULL ON UPDATE CASCADE
);

-- ============================================
-- ÍNDICES PARA OPTIMIZACIÓN DE CONSULTAS
-- ============================================

CREATE INDEX idx_rental_customer_id ON rental(customer_id);
CREATE INDEX idx_rental_inventory_id ON rental(inventory_id);
CREATE INDEX idx_rental_staff_id ON rental(staff_id);
CREATE INDEX idx_rental_date ON rental(rental_date);
CREATE INDEX idx_rental_return_date ON rental(return_date);

CREATE INDEX idx_payment_customer_id ON payment(customer_id);
CREATE INDEX idx_payment_staff_id ON payment(staff_id);
CREATE INDEX idx_payment_rental_id ON payment(rental_id);
CREATE INDEX idx_payment_date ON payment(payment_date);

CREATE INDEX idx_inventory_film_id ON inventory(film_id);
CREATE INDEX idx_inventory_store_id ON inventory(store_id);

CREATE INDEX idx_film_language_id ON film(language_id);
CREATE INDEX idx_film_title ON film(title);

CREATE INDEX idx_film_actor_film_id ON film_actor(film_id);
CREATE INDEX idx_film_actor_actor_id ON film_actor(actor_id);

CREATE INDEX idx_film_category_film_id ON film_category(film_id);
CREATE INDEX idx_film_category_category_id ON film_category(category_id);

CREATE INDEX idx_customer_last_name ON customer(last_name);
CREATE INDEX idx_customer_store_id ON customer(store_id);
CREATE INDEX idx_customer_address_id ON customer(address_id);

CREATE INDEX idx_address_city_id ON address(city_id);
CREATE INDEX idx_city_country_id ON city(country_id);
CREATE INDEX idx_actor_last_name ON actor(last_name);