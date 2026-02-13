# Laboratorio 1: Sakila DVD Rental - Análisis de Base de Datos

**Estudiante**: Daniel Rosas
**Fecha de entrega**: 13 de Febrero 2026  
**Curso**: ADD  

---

## 📋 Descripción del Proyecto

Sistema de análisis de datos para **Sakila DVD Rental**, una cadena de tiendas de alquiler de películas en DVD.
---

## 🗂️ Estructura del Proyecto

```
lab01/
├── compose.yaml                    # Orquestación de contenedores
├── db/
│   └── init/
│       ├── 01_sakila_schema.sql   # Esquema de tablas
│       └── 02_sakila_data.sql     # Datos (~46,000 líneas)
├── web/
│   ├── app.py                     # Aplicación Flask
│   ├── model.py                   # Modelos SQLAlchemy (ORM)
│   ├── queries.py                 # 4 consultas de negocio
│   ├── Dockerfile                 # Imagen del contenedor web
│   └── requirements.txt           # Dependencias Python
├── DIAGRAMA_BD.md                 # Diagrama ER en Mermaid
├── SOLUCIONES_NEGOCIO.md          # Documentación de consultas
├── DIAGRAMA_MERMAID.svg           # Foto delDiagrama ER en Mermaid
└── README.md                      # Este archivo
```

---

## 🚀 Instalación y Ejecución

### Iniciar el proyecto

```bash
# 1. Clonar o descargar el repositorio
cd lab01

# 2. Iniciar contenedores
docker compose up -d --build

# 3. Esperar ~60 segundos para carga de datos

# 4. Acceder a la aplicación
# Abrir navegador: http://localhost:8000
```

### Verificar funcionamiento

```bash
# Ver contenedores corriendo
docker ps

# Ver logs
docker compose logs -f

# Conectarse a PostgreSQL
docker exec -it lab01_sakila_postgres psql -U postgres -d sakila
```

---


## 🗄️ Modelo de Datos

### Entidades Principales (13 tablas)

- **Film** (1,000 películas)
- **Actor** (200 actores)
- **Category** (16 categorías)
- **Customer** (599 clientes)
- **Rental** (~16,000 alquileres)
- **Payment** (~16,000 pagos)
- **Inventory** (~4,500 copias)
- **Store, Staff, Address, City, Country, Language**

Ver diagrama completo en: [DIAGRAMA_BD.md](DIAGRAMA_BD.md)

---

## 📝 Documentación

- **[DIAGRAMA_BD.md](DIAGRAMA_BD.md)**: Diagrama ER completo
- **[SOLUCIONES_NEGOCIO.md](SOLUCIONES_NEGOCIO.md)**: Explicación detallada de las 4 consultas

---

## 🔧 Comandos Útiles

```bash
# Ver logs
docker compose logs -f

# Reiniciar servicios
docker compose restart

# Detener (mantiene datos)
docker compose down

# Detener y eliminar datos
docker compose down -v

# Conectar a PostgreSQL
docker exec -it lab01_sakila_postgres psql -U postgres -d sakila
```

---

## 📈 Acceso al Sistema

- **Aplicación Web**: http://localhost:8000
- **PostgreSQL**: localhost:5432
  - Usuario: `postgres`
  - Contraseña: `1234`
  - Base de datos: `sakila`
- **Health Check**: http://localhost:8000/health
- **API Stats**: http://localhost:8000/api/stats


---

## 📄 Nota sobre la Implementación

Aunque el laboratorio sugiere Podman, se utilizó **Docker Desktop** debido a problemas en Windows, recomendado por una IA. Los archivos de configuración son idénticos y compatibles con ambas herramientas.

Se utilizo ia la parte D y su documento,  ademas de la modificar algunos archivos que daba error.

---


## 👨‍💻 Autor

Daniel Rosas
danielrosaso@javeriana.edu.co
Febrero 2026
