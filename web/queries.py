"""
Consultas de Negocio para Sakila DVD Rental
Implementadas con SQLAlchemy ORM
"""

from sqlalchemy import func, and_, extract, distinct
from sqlalchemy.orm import Session
from model import (
    Film, Category, FilmCategory, Customer, Rental, 
    Payment, Inventory
)


def peliculas_mas_alquiladas_por_categoria(db: Session):
    """
    PREGUNTA 1: ¿Cuáles son las películas con más alquileres por categoría?
    
    Retorna la película más alquilada de cada categoría
    """
    
    # Subconsulta para contar alquileres por película y categoría
    rental_count_subq = (
        db.query(
            Film.film_id,
            Film.title,
            Category.category_id,
            Category.name.label('category_name'),
            func.count(Rental.rental_id).label('rental_count')
        )
        .join(Inventory, Film.film_id == Inventory.film_id)
        .join(Rental, Inventory.inventory_id == Rental.inventory_id)
        .join(FilmCategory, Film.film_id == FilmCategory.film_id)
        .join(Category, FilmCategory.category_id == Category.category_id)
        .group_by(Film.film_id, Film.title, Category.category_id, Category.name)
        .subquery()
    )
    
    # Subconsulta para obtener el máximo de alquileres por categoría
    max_rentals_subq = (
        db.query(
            rental_count_subq.c.category_id,
            func.max(rental_count_subq.c.rental_count).label('max_rentals')
        )
        .group_by(rental_count_subq.c.category_id)
        .subquery()
    )
    
    # Consulta final: películas con el máximo de alquileres en su categoría
    result = (
        db.query(
            rental_count_subq.c.title,
            rental_count_subq.c.category_name,
            rental_count_subq.c.rental_count
        )
        .join(
            max_rentals_subq,
            and_(
                rental_count_subq.c.category_id == max_rentals_subq.c.category_id,
                rental_count_subq.c.rental_count == max_rentals_subq.c.max_rentals
            )
        )
        .order_by(rental_count_subq.c.category_name, rental_count_subq.c.rental_count.desc())
        .all()
    )
    
    return result


def clientes_gasto_superior_promedio(db: Session):
    """
    PREGUNTA 2: ¿Cuáles son los clientes cuyo gasto total es superior al promedio?
    
    Calcula el gasto promedio de todos los clientes y retorna aquellos
    que gastaron más que ese promedio
    """
    
    # Subconsulta para calcular el gasto total por cliente
    customer_spending = (
        db.query(
            Customer.customer_id,
            func.sum(Payment.amount).label('total_spending')
        )
        .join(Payment, Customer.customer_id == Payment.customer_id)
        .group_by(Customer.customer_id)
        .subquery()
    )
    
    # Calcular el promedio de gastos
    avg_spending = db.query(
        func.avg(customer_spending.c.total_spending)
    ).scalar()
    
    # Consulta principal: clientes con gasto superior al promedio
    result = (
        db.query(
            Customer.customer_id,
            Customer.first_name,
            Customer.last_name,
            Customer.email,
            func.sum(Payment.amount).label('total_spending')
        )
        .join(Payment, Customer.customer_id == Payment.customer_id)
        .group_by(
            Customer.customer_id,
            Customer.first_name,
            Customer.last_name,
            Customer.email
        )
        .having(func.sum(Payment.amount) > avg_spending)
        .order_by(func.sum(Payment.amount).desc())
        .all()
    )
    
    return result, avg_spending


def peliculas_mas_alquiladas_que_promedio_categoria(db: Session):
    """
    PREGUNTA 3: ¿Cuáles son las películas más alquiladas que el promedio de su categoría?
    
    Para cada categoría, encuentra películas que fueron alquiladas más veces
    que el promedio de alquileres de esa categoría
    """
    
    # Subconsulta: contar alquileres por película
    film_rental_count = (
        db.query(
            Film.film_id,
            func.count(Rental.rental_id).label('rental_count')
        )
        .join(Inventory, Film.film_id == Inventory.film_id)
        .join(Rental, Inventory.inventory_id == Rental.inventory_id)
        .group_by(Film.film_id)
        .subquery()
    )
    
    # Subconsulta: calcular promedio de alquileres por categoría
    category_avg_rentals = (
        db.query(
            FilmCategory.category_id,
            func.avg(film_rental_count.c.rental_count).label('avg_rentals')
        )
        .join(film_rental_count, FilmCategory.film_id == film_rental_count.c.film_id)
        .group_by(FilmCategory.category_id)
        .subquery()
    )
    
    # Consulta principal
    result = (
        db.query(
            Film.title,
            Category.name.label('category_name'),
            film_rental_count.c.rental_count
        )
        .join(film_rental_count, Film.film_id == film_rental_count.c.film_id)
        .join(FilmCategory, Film.film_id == FilmCategory.film_id)
        .join(Category, FilmCategory.category_id == Category.category_id)
        .join(
            category_avg_rentals,
            FilmCategory.category_id == category_avg_rentals.c.category_id
        )
        .filter(film_rental_count.c.rental_count > category_avg_rentals.c.avg_rentals)
        .order_by(Category.name, film_rental_count.c.rental_count.desc())
        .all()
    )
    
    return result


def clientes_q1_no_q2(db: Session, year: int = 2005):
    """
    PREGUNTA 4: ¿Cuáles son los clientes que alquilaron en el primer trimestre 
    pero no en el segundo trimestre?
    
    Identifica clientes que estuvieron activos en Q1 pero no en Q2 del año especificado
    """
    
    # Subconsulta: clientes que alquilaron en Q1
    q1_customers = (
        db.query(distinct(Customer.customer_id))
        .join(Rental, Customer.customer_id == Rental.customer_id)
        .filter(
            extract('year', Rental.rental_date) == year,
            extract('quarter', Rental.rental_date) == 1
        )
        .subquery()
    )
    
    # Subconsulta: clientes que alquilaron en Q2
    q2_customers = (
        db.query(distinct(Customer.customer_id))
        .join(Rental, Customer.customer_id == Rental.customer_id)
        .filter(
            extract('year', Rental.rental_date) == year,
            extract('quarter', Rental.rental_date) == 2
        )
        .subquery()
    )
    
    # Consulta principal: clientes en Q1 pero NO en Q2
    result = (
        db.query(
            Customer.customer_id,
            Customer.first_name,
            Customer.last_name,
            Customer.email,
            func.count(Rental.rental_id).label('q1_rentals')
        )
        .join(Rental, Customer.customer_id == Rental.customer_id)
        .filter(
            Customer.customer_id.in_(q1_customers),
            ~Customer.customer_id.in_(q2_customers),
            extract('year', Rental.rental_date) == year,
            extract('quarter', Rental.rental_date) == 1
        )
        .group_by(
            Customer.customer_id,
            Customer.first_name,
            Customer.last_name,
            Customer.email
        )
        .order_by(Customer.last_name, Customer.first_name)
        .all()
    )
    
    return result