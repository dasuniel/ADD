"""
Modelos SQLAlchemy para la base de datos Sakila
DVD Rental Store Chain
"""

from sqlalchemy import (
    Column, Integer, String, Text, Numeric, Boolean, DateTime, 
    Date, ForeignKey, SmallInteger, CHAR, LargeBinary, TIMESTAMP
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from sqlalchemy import create_engine
from datetime import datetime
import os

Base = declarative_base()


class Language(Base):
    """Idiomas disponibles para películas"""
    __tablename__ = 'language'
    
    language_id = Column(SmallInteger, primary_key=True)
    name = Column(CHAR(20), nullable=False)
    last_update = Column(TIMESTAMP, nullable=False, default=datetime.now)
    
    films = relationship('Film', foreign_keys='Film.language_id', back_populates='language')
    films_original = relationship('Film', foreign_keys='Film.original_language_id', back_populates='original_language')


class Category(Base):
    """Categorías de películas"""
    __tablename__ = 'category'
    
    category_id = Column(SmallInteger, primary_key=True)
    name = Column(String(25), nullable=False)
    last_update = Column(TIMESTAMP, nullable=False, default=datetime.now)
    
    film_categories = relationship('FilmCategory', back_populates='category')


class Country(Base):
    """Países"""
    __tablename__ = 'country'
    
    country_id = Column(SmallInteger, primary_key=True)
    country = Column(String(50), nullable=False)
    last_update = Column(TIMESTAMP, nullable=False, default=datetime.now)
    
    cities = relationship('City', back_populates='country')


class City(Base):
    """Ciudades"""
    __tablename__ = 'city'
    
    city_id = Column(SmallInteger, primary_key=True)
    city = Column(String(50), nullable=False)
    country_id = Column(SmallInteger, ForeignKey('country.country_id'), nullable=False)
    last_update = Column(TIMESTAMP, nullable=False, default=datetime.now)
    
    country = relationship('Country', back_populates='cities')
    addresses = relationship('Address', back_populates='city')


class Address(Base):
    """Direcciones"""
    __tablename__ = 'address'
    
    address_id = Column(SmallInteger, primary_key=True)
    address = Column(String(50), nullable=False)
    address2 = Column(String(50))
    district = Column(String(20), nullable=False)
    city_id = Column(SmallInteger, ForeignKey('city.city_id'), nullable=False)
    postal_code = Column(String(10))
    phone = Column(String(20), nullable=False)
    last_update = Column(TIMESTAMP, nullable=False, default=datetime.now)
    
    city = relationship('City', back_populates='addresses')
    customers = relationship('Customer', back_populates='address')
    staff_members = relationship('Staff', back_populates='address')
    stores = relationship('Store', back_populates='address')


class Store(Base):
    """Tiendas"""
    __tablename__ = 'store'
    
    store_id = Column(SmallInteger, primary_key=True)
    manager_staff_id = Column(SmallInteger, ForeignKey('staff.staff_id'), nullable=False)
    address_id = Column(SmallInteger, ForeignKey('address.address_id'), nullable=False)
    last_update = Column(TIMESTAMP, nullable=False, default=datetime.now)
    
    address = relationship('Address', back_populates='stores')
    manager = relationship('Staff', foreign_keys=[manager_staff_id], back_populates='managed_store')
    staff_members = relationship('Staff', foreign_keys='Staff.store_id', back_populates='store')
    customers = relationship('Customer', back_populates='store')
    inventory_items = relationship('Inventory', back_populates='store')


class Staff(Base):
    """Personal/Empleados"""
    __tablename__ = 'staff'
    
    staff_id = Column(SmallInteger, primary_key=True)
    first_name = Column(String(45), nullable=False)
    last_name = Column(String(45), nullable=False)
    address_id = Column(SmallInteger, ForeignKey('address.address_id'), nullable=False)
    picture = Column(LargeBinary)
    email = Column(String(50))
    store_id = Column(SmallInteger, ForeignKey('store.store_id'), nullable=False)
    active = Column(Boolean, nullable=False, default=True)
    username = Column(String(16), nullable=False)
    password = Column(String(40))
    last_update = Column(TIMESTAMP, nullable=False, default=datetime.now)
    
    address = relationship('Address', back_populates='staff_members')
    store = relationship('Store', foreign_keys=[store_id], back_populates='staff_members')
    managed_store = relationship('Store', foreign_keys='Store.manager_staff_id', back_populates='manager', uselist=False)
    rentals = relationship('Rental', back_populates='staff')
    payments = relationship('Payment', back_populates='staff')


class Customer(Base):
    """Clientes"""
    __tablename__ = 'customer'
    
    customer_id = Column(SmallInteger, primary_key=True)
    store_id = Column(SmallInteger, ForeignKey('store.store_id'), nullable=False)
    first_name = Column(String(45), nullable=False)
    last_name = Column(String(45), nullable=False)
    email = Column(String(50))
    address_id = Column(SmallInteger, ForeignKey('address.address_id'), nullable=False)
    active = Column(Boolean, nullable=False, default=True)
    create_date = Column(Date, nullable=False, default=datetime.now().date)
    last_update = Column(TIMESTAMP, default=datetime.now)
    
    store = relationship('Store', back_populates='customers')
    address = relationship('Address', back_populates='customers')
    rentals = relationship('Rental', back_populates='customer')
    payments = relationship('Payment', back_populates='customer')


class Actor(Base):
    """Actores"""
    __tablename__ = 'actor'
    
    actor_id = Column(SmallInteger, primary_key=True)
    first_name = Column(String(45), nullable=False)
    last_name = Column(String(45), nullable=False)
    last_update = Column(TIMESTAMP, nullable=False, default=datetime.now)
    
    film_actors = relationship('FilmActor', back_populates='actor')


class Film(Base):
    """Películas"""
    __tablename__ = 'film'
    
    film_id = Column(SmallInteger, primary_key=True)
    title = Column(String(255), nullable=False)
    description = Column(Text)
    release_year = Column(Integer)
    language_id = Column(SmallInteger, ForeignKey('language.language_id'), nullable=False)
    original_language_id = Column(SmallInteger, ForeignKey('language.language_id'))
    rental_duration = Column(SmallInteger, nullable=False, default=3)
    rental_rate = Column(Numeric(4, 2), nullable=False, default=4.99)
    length = Column(SmallInteger)
    replacement_cost = Column(Numeric(5, 2), nullable=False, default=19.99)
    rating = Column(String(10), default='G')
    special_features = Column(Text)
    last_update = Column(TIMESTAMP, nullable=False, default=datetime.now)
    
    language = relationship('Language', foreign_keys=[language_id], back_populates='films')
    original_language = relationship('Language', foreign_keys=[original_language_id], back_populates='films_original')
    film_actors = relationship('FilmActor', back_populates='film')
    film_categories = relationship('FilmCategory', back_populates='film')
    inventory_items = relationship('Inventory', back_populates='film')


class FilmActor(Base):
    """Relación muchos a muchos entre Film y Actor"""
    __tablename__ = 'film_actor'
    
    actor_id = Column(SmallInteger, ForeignKey('actor.actor_id'), primary_key=True)
    film_id = Column(SmallInteger, ForeignKey('film.film_id'), primary_key=True)
    last_update = Column(TIMESTAMP, nullable=False, default=datetime.now)
    
    actor = relationship('Actor', back_populates='film_actors')
    film = relationship('Film', back_populates='film_actors')


class FilmCategory(Base):
    """Relación muchos a muchos entre Film y Category"""
    __tablename__ = 'film_category'
    
    film_id = Column(SmallInteger, ForeignKey('film.film_id'), primary_key=True)
    category_id = Column(SmallInteger, ForeignKey('category.category_id'), primary_key=True)
    last_update = Column(TIMESTAMP, nullable=False, default=datetime.now)
    
    film = relationship('Film', back_populates='film_categories')
    category = relationship('Category', back_populates='film_categories')


class Inventory(Base):
    """Inventario de películas en tiendas"""
    __tablename__ = 'inventory'
    
    inventory_id = Column(Integer, primary_key=True)
    film_id = Column(SmallInteger, ForeignKey('film.film_id'), nullable=False)
    store_id = Column(SmallInteger, ForeignKey('store.store_id'), nullable=False)
    last_update = Column(TIMESTAMP, nullable=False, default=datetime.now)
    
    film = relationship('Film', back_populates='inventory_items')
    store = relationship('Store', back_populates='inventory_items')
    rentals = relationship('Rental', back_populates='inventory')


class Rental(Base):
    """Alquileres de películas"""
    __tablename__ = 'rental'
    
    rental_id = Column(Integer, primary_key=True)
    rental_date = Column(TIMESTAMP, nullable=False)
    inventory_id = Column(Integer, ForeignKey('inventory.inventory_id'), nullable=False)
    customer_id = Column(SmallInteger, ForeignKey('customer.customer_id'), nullable=False)
    return_date = Column(TIMESTAMP)
    staff_id = Column(SmallInteger, ForeignKey('staff.staff_id'), nullable=False)
    last_update = Column(TIMESTAMP, nullable=False, default=datetime.now)
    
    inventory = relationship('Inventory', back_populates='rentals')
    customer = relationship('Customer', back_populates='rentals')
    staff = relationship('Staff', back_populates='rentals')
    payments = relationship('Payment', back_populates='rental')


class Payment(Base):
    """Pagos realizados por alquileres"""
    __tablename__ = 'payment'
    
    payment_id = Column(Integer, primary_key=True)
    customer_id = Column(SmallInteger, ForeignKey('customer.customer_id'), nullable=False)
    staff_id = Column(SmallInteger, ForeignKey('staff.staff_id'), nullable=False)
    rental_id = Column(Integer, ForeignKey('rental.rental_id'))
    amount = Column(Numeric(5, 2), nullable=False)
    payment_date = Column(TIMESTAMP, nullable=False)
    last_update = Column(TIMESTAMP, default=datetime.now)
    
    customer = relationship('Customer', back_populates='payments')
    staff = relationship('Staff', back_populates='payments')
    rental = relationship('Rental', back_populates='payments')


# Configuración de la base de datos
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres:1234@db:5432/sakila"
)

engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    """Generator para obtener sesiones de base de datos"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()