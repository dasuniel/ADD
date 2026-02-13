"""
Aplicación Web Flask para Sakila DVD Rental
Sistema de consultas de negocio y análisis
"""

import os
from flask import Flask, request, render_template_string, jsonify
from sqlalchemy import create_engine, text
from model import SessionLocal, get_db
import queries

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres:1234@db:5432/sakila"
)

engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)
app = Flask(__name__)

HTML_TEMPLATE = """
<!doctype html>
<html lang="es">
<head>
    <meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Sakila DVD Rental - Sistema de Análisis</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body { 
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        
        .container {
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            padding: 40px;
            border-radius: 20px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
        }
        
        header {
            text-align: center;
            margin-bottom: 40px;
            padding-bottom: 20px;
            border-bottom: 3px solid #667eea;
        }
        
        h1 {
            color: #2c3e50;
            font-size: 2.5em;
            margin-bottom: 10px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }
        
        .subtitle {
            color: #7f8c8d;
            font-size: 1.1em;
        }
        
        h2 {
            color: #34495e;
            margin: 30px 0 15px 0;
            font-size: 1.5em;
            display: flex;
            align-items: center;
        }
        
        h2:before {
            content: "📊";
            margin-right: 10px;
            font-size: 1.2em;
        }
        
        .query-box {
            background: #f8f9fa;
            border: 2px solid #e9ecef;
            padding: 25px;
            margin: 25px 0;
            border-radius: 15px;
            transition: all 0.3s ease;
        }
        
        .query-box:hover {
            border-color: #667eea;
            box-shadow: 0 5px 15px rgba(102, 126, 234, 0.2);
        }
        
        .query-description {
            color: #6c757d;
            font-size: 0.95em;
            margin-bottom: 15px;
            padding: 10px;
            background: white;
            border-left: 4px solid #667eea;
            border-radius: 5px;
        }
        
        button {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border: none;
            padding: 12px 30px;
            border-radius: 8px;
            cursor: pointer;
            font-size: 16px;
            font-weight: 600;
            transition: all 0.3s ease;
            box-shadow: 0 4px 15px rgba(102, 126, 234, 0.4);
        }
        
        button:hover {
            transform: translateY(-2px);
            box-shadow: 0 6px 20px rgba(102, 126, 234, 0.6);
        }
        
        button:active {
            transform: translateY(0);
        }
        
        table {
            border-collapse: collapse;
            width: 100%;
            margin-top: 20px;
            background: white;
            border-radius: 10px;
            overflow: hidden;
            box-shadow: 0 0 20px rgba(0,0,0,0.1);
        }
        
        th, td {
            padding: 15px;
            text-align: left;
            border-bottom: 1px solid #e9ecef;
        }
        
        th {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            font-weight: 600;
            text-transform: uppercase;
            font-size: 0.85em;
            letter-spacing: 1px;
        }
        
        tr:hover {
            background: #f8f9fa;
        }
        
        tr:last-child td {
            border-bottom: none;
        }
        
        .metric {
            background: linear-gradient(135deg, #667eea15 0%, #764ba215 100%);
            padding: 15px 20px;
            border-radius: 10px;
            margin: 15px 0;
            font-weight: 600;
            color: #2c3e50;
            border-left: 4px solid #667eea;
        }
        
        .metric span {
            color: #667eea;
            font-size: 1.2em;
        }
        
        .no-results {
            text-align: center;
            padding: 40px;
            color: #95a5a6;
            font-size: 1.1em;
        }
        
        footer {
            text-align: center;
            margin-top: 40px;
            padding-top: 20px;
            border-top: 2px solid #e9ecef;
            color: #7f8c8d;
        }
        
        @media (max-width: 768px) {
            .container {
                padding: 20px;
            }
            
            h1 {
                font-size: 1.8em;
            }
            
            table {
                font-size: 0.9em;
            }
            
            th, td {
                padding: 10px;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>🎬 Sakila DVD Rental</h1>
            <p class="subtitle">Sistema de Análisis de Negocio - Base de Datos Relacional</p>
        </header>
        
        <div class="query-box">
            <h2>Películas con más alquileres por categoría</h2>
            <p class="query-description">
                Identifica la película más popular de cada categoría basándose en el número total de alquileres.
                Útil para optimizar inventario y entender preferencias por género.
            </p>
            <form method="get" action="/">
                <input type="hidden" name="query" value="top_films_by_category"/>
                <button type="submit">🔍 Ejecutar Consulta</button>
            </form>
            {% if query == "top_films_by_category" %}
                {% if results %}
                    <table>
                        <thead>
                            <tr>
                                <th>Película</th>
                                <th>Categoría</th>
                                <th>Total Alquileres</th>
                            </tr>
                        </thead>
                        <tbody>
                            {% for r in results %}
                            <tr>
                                <td><strong>{{ r[0] }}</strong></td>
                                <td>{{ r[1] }}</td>
                                <td>{{ r[2] }}</td>
                            </tr>
                            {% endfor %}
                        </tbody>
                    </table>
                    <div class="metric">
                        Total de categorías analizadas: <span>{{ results|length }}</span>
                    </div>
                {% else %}
                    <p class="no-results">No se encontraron resultados</p>
                {% endif %}
            {% endif %}
        </div>
        
        <div class="query-box">
            <h2>Clientes con gasto superior al promedio</h2>
            <p class="query-description">
                Lista clientes VIP que han gastado más que el promedio general. 
                Ideal para programas de fidelización y marketing dirigido.
            </p>
            <form method="get" action="/">
                <input type="hidden" name="query" value="high_spending_customers"/>
                <button type="submit">💰 Ejecutar Consulta</button>
            </form>
            {% if query == "high_spending_customers" %}
                {% if avg_spending %}
                    <div class="metric">
                        💳 Gasto Promedio General: <span>${{ "%.2f"|format(avg_spending) }}</span>
                    </div>
                {% endif %}
                {% if results %}
                    <table>
                        <thead>
                            <tr>
                                <th>ID</th>
                                <th>Nombre</th>
                                <th>Apellido</th>
                                <th>Email</th>
                                <th>Gasto Total</th>
                            </tr>
                        </thead>
                        <tbody>
                            {% for r in results %}
                            <tr>
                                <td>{{ r[0] }}</td>
                                <td>{{ r[1] }}</td>
                                <td>{{ r[2] }}</td>
                                <td><em>{{ r[3] or 'N/A' }}</em></td>
                                <td><strong>${{ "%.2f"|format(r[4]) }}</strong></td>
                            </tr>
                            {% endfor %}
                        </tbody>
                    </table>
                    <div class="metric">
                        Total de clientes VIP: <span>{{ results|length }}</span>
                    </div>
                {% else %}
                    <p class="no-results">No se encontraron resultados</p>
                {% endif %}
            {% endif %}
        </div>
        
        <div class="query-box">
            <h2>Películas más populares que el promedio de su categoría</h2>
            <p class="query-description">
                Encuentra películas excepcionales que superan el rendimiento promedio dentro de su categoría.
                Ayuda a identificar títulos destacados para promociones especiales.
            </p>
            <form method="get" action="/">
                <input type="hidden" name="query" value="above_avg_films"/>
                <button type="submit">⭐ Ejecutar Consulta</button>
            </form>
            {% if query == "above_avg_films" %}
                {% if results %}
                    <table>
                        <thead>
                            <tr>
                                <th>Película</th>
                                <th>Categoría</th>
                                <th>Alquileres</th>
                            </tr>
                        </thead>
                        <tbody>
                            {% for r in results %}
                            <tr>
                                <td><strong>{{ r[0] }}</strong></td>
                                <td>{{ r[1] }}</td>
                                <td>{{ r[2] }}</td>
                            </tr>
                            {% endfor %}
                        </tbody>
                    </table>
                    <div class="metric">
                        Películas destacadas: <span>{{ results|length }}</span>
                    </div>
                {% else %}
                    <p class="no-results">No se encontraron resultados</p>
                {% endif %}
            {% endif %}
        </div>
        
        <div class="query-box">
            <h2>Clientes Q1 que no alquilaron en Q2</h2>
            <p class="query-description">
                Identifica clientes que estuvieron activos en el primer trimestre pero no regresaron en el segundo.
                Útil para campañas de reactivación y análisis de retención.
            </p>
            <form method="get" action="/">
                <input type="hidden" name="query" value="q1_not_q2"/>
                <button type="submit">📅 Ejecutar Consulta</button>
            </form>
            {% if query == "q1_not_q2" %}
                {% if results %}
                    <table>
                        <thead>
                            <tr>
                                <th>ID</th>
                                <th>Nombre</th>
                                <th>Apellido</th>
                                <th>Email</th>
                                <th>Alquileres Q1</th>
                            </tr>
                        </thead>
                        <tbody>
                            {% for r in results %}
                            <tr>
                                <td>{{ r[0] }}</td>
                                <td>{{ r[1] }}</td>
                                <td>{{ r[2] }}</td>
                                <td><em>{{ r[3] or 'N/A' }}</em></td>
                                <td>{{ r[4] }}</td>
                            </tr>
                            {% endfor %}
                        </tbody>
                    </table>
                    <div class="metric">
                        Clientes a reactivar: <span>{{ results|length }}</span>
                    </div>
                {% else %}
                    <p class="no-results">No se encontraron clientes con este patrón</p>
                {% endif %}
            {% endif %}
        </div>
        
        <footer>
            <p>📊 Laboratorio 1 - Bases de Datos | SQLAlchemy ORM + PostgreSQL</p>
        </footer>
    </div>
</body>
</html>
"""

@app.get("/")
def index():
    """Ruta principal con interfaz de consultas"""
    query_type = request.args.get("query", "")
    results = []
    avg_spending = None
    
    db = next(get_db())
    
    try:
        if query_type == "top_films_by_category":
            results = queries.peliculas_mas_alquiladas_por_categoria(db)
            
        elif query_type == "high_spending_customers":
            results, avg_spending = queries.clientes_gasto_superior_promedio(db)
            
        elif query_type == "above_avg_films":
            results = queries.peliculas_mas_alquiladas_que_promedio_categoria(db)
            
        elif query_type == "q1_not_q2":
            results = queries.clientes_q1_no_q2(db, year=2005)
            
    except Exception as e:
        print(f"Error en consulta: {e}")
        results = []
    finally:
        db.close()
    
    return render_template_string(
        HTML_TEMPLATE,
        query=query_type,
        results=results,
        avg_spending=avg_spending
    )


@app.get("/health")
def health():
    """Endpoint de salud para verificar conexión a la base de datos"""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "ok", "database": "connected"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}, 500


@app.get("/api/stats")
def api_stats():
    """API para estadísticas generales"""
    db = next(get_db())
    
    try:
        from model import Film, Customer, Rental, Payment, Category
        
        stats = {
            "total_films": db.query(Film).count(),
            "total_customers": db.query(Customer).count(),
            "total_rentals": db.query(Rental).count(),
            "total_revenue": float(db.query(func.sum(Payment.amount)).scalar() or 0),
            "total_categories": db.query(Category).count()
        }
        
        return jsonify(stats)
    except Exception as e:
        return {"error": str(e)}, 500
    finally:
        db.close()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)