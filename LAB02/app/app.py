"""
AdventureWorks Analytics - Aplicación Web Flask
Presenta los resultados de las 4 preguntas de negocio del DW.
"""
import os
import sys
import logging
import subprocess
from decimal import Decimal
from datetime import datetime

from flask import Flask, render_template_string, jsonify, request
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# ── Configuración básica ────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "lab02-secret")

# ── Conexión al DW (OLAP) ───────────────────────────────────────────────────
OLAP_URL = (
    f"postgresql+psycopg2://"
    f"{os.getenv('OLAP_USER','postgres')}:{os.getenv('OLAP_PASSWORD','postgres123')}"
    f"@{os.getenv('OLAP_HOST','olap_db')}:{os.getenv('OLAP_PORT','5432')}"
    f"/{os.getenv('OLAP_DB','adventureworks_dw')}"
)

_engine = None

def get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(OLAP_URL, pool_pre_ping=True)
    return _engine

def query_dw(sql: str, params: dict = None) -> list[dict]:
    """Ejecuta un query en el DW y retorna lista de dicts."""
    with get_engine().connect() as conn:
        result = conn.execute(text(sql), params or {})
        keys = list(result.keys())
        return [dict(zip(keys, row)) for row in result]

def fmt_number(value) -> str:
    """Formatea un número con separadores de miles."""
    try:
        f = float(value)
        if f == int(f):
            return f"{int(f):,}"
        return f"{f:,.2f}"
    except Exception:
        return str(value)

# ── HTML Template ───────────────────────────────────────────────────────────
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>AdventureWorks Analytics - Lab02</title>
  <style>
    :root {
      --primary:   #1e3a5f;
      --accent:    #e85d04;
      --bg:        #f0f4f8;
      --card-bg:   #ffffff;
      --border:    #d1dce8;
      --text:      #1a202c;
      --muted:     #6b7a8d;
      --success:   #2d6a4f;
      --success-bg:#d8f3dc;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: 'Segoe UI', system-ui, sans-serif; background: var(--bg); color: var(--text); }

    /* ── Header ── */
    header {
      background: linear-gradient(135deg, var(--primary) 0%, #2d5986 100%);
      color: white; padding: 24px 40px;
      display: flex; align-items: center; gap: 20px;
      box-shadow: 0 3px 12px rgba(0,0,0,.25);
    }
    header .logo { font-size: 2.4rem; }
    header h1 { font-size: 1.6rem; font-weight: 700; line-height: 1.2; }
    header p  { font-size: .9rem; opacity: .8; margin-top: 4px; }
    header .badge {
      margin-left: auto; background: var(--accent); color: white;
      padding: 6px 14px; border-radius: 20px; font-size: .8rem; font-weight: 600;
    }

    /* ── Layout ── */
    main { max-width: 1200px; margin: 0 auto; padding: 32px 24px; }

    /* ── ETL Banner ── */
    .etl-banner {
      background: var(--primary); color: white;
      border-radius: 10px; padding: 16px 24px;
      display: flex; align-items: center; justify-content: space-between;
      margin-bottom: 28px; box-shadow: 0 2px 8px rgba(0,0,0,.15);
    }
    .etl-banner span { font-size: .95rem; }
    .btn-etl {
      background: var(--accent); color: white; border: none;
      padding: 10px 22px; border-radius: 6px; cursor: pointer;
      font-size: .9rem; font-weight: 600; transition: background .2s;
    }
    .btn-etl:hover { background: #c44b00; }

    /* ── Cards ── */
    .card {
      background: var(--card-bg); border: 1px solid var(--border);
      border-radius: 12px; padding: 28px;
      margin-bottom: 28px; box-shadow: 0 2px 8px rgba(0,0,0,.06);
    }
    .card-header {
      display: flex; align-items: flex-start;
      justify-content: space-between; margin-bottom: 18px;
      gap: 12px; flex-wrap: wrap;
    }
    .card-title-group { flex: 1; }
    .card-number {
      width: 34px; height: 34px; border-radius: 50%;
      background: var(--primary); color: white;
      display: flex; align-items: center; justify-content: center;
      font-weight: 700; font-size: .9rem; margin-bottom: 8px;
    }
    .card-title   { font-size: 1.15rem; font-weight: 700; color: var(--primary); }
    .card-subtitle{ font-size: .85rem; color: var(--muted); margin-top: 4px; }
    .btn-query {
      background: var(--primary); color: white; border: none;
      padding: 10px 20px; border-radius: 8px; cursor: pointer;
      font-size: .88rem; font-weight: 600; white-space: nowrap;
      transition: background .2s, transform .1s;
    }
    .btn-query:hover  { background: #2d5986; }
    .btn-query:active { transform: scale(.97); }
    .btn-query:disabled { background: #94a3b8; cursor: not-allowed; }

    /* ── Results ── */
    .result-area { margin-top: 18px; }
    .result-area.hidden { display: none; }
    .loading {
      text-align: center; padding: 24px; color: var(--muted);
      font-size: .9rem;
    }
    .loading::before {
      content: '';
      display: inline-block; width: 20px; height: 20px;
      border: 3px solid var(--border); border-top-color: var(--primary);
      border-radius: 50%; animation: spin .8s linear infinite;
      margin-right: 10px; vertical-align: middle;
    }
    @keyframes spin { to { transform: rotate(360deg); } }

    /* ── Tables ── */
    .result-table { width: 100%; border-collapse: collapse; font-size: .88rem; }
    .result-table thead { background: var(--primary); color: white; }
    .result-table th { padding: 11px 14px; text-align: left; font-weight: 600; white-space: nowrap; }
    .result-table td { padding: 10px 14px; border-bottom: 1px solid var(--border); }
    .result-table tbody tr:hover { background: #f1f5f9; }
    .result-table tbody tr:last-child td { border-bottom: none; }
    .num { text-align: right; font-variant-numeric: tabular-nums; }
    .badge-pill {
      display: inline-block; padding: 3px 10px; border-radius: 12px;
      font-size: .78rem; font-weight: 600;
    }
    .badge-recurring { background: var(--success-bg); color: var(--success); }
    .badge-onetime   { background: #fce4ec; color: #b71c1c; }
    .badge-rank      { background: #e3f2fd; color: #1565c0; }

    /* ── Summary stats ── */
    .stats-row {
      display: flex; gap: 16px; margin-bottom: 18px; flex-wrap: wrap;
    }
    .stat-box {
      flex: 1; min-width: 140px;
      background: linear-gradient(135deg, var(--primary), #2d5986);
      color: white; border-radius: 10px; padding: 16px;
      text-align: center;
    }
    .stat-box .stat-value { font-size: 1.5rem; font-weight: 700; }
    .stat-box .stat-label { font-size: .75rem; opacity: .85; margin-top: 4px; }

    /* ── Footer ── */
    footer {
      text-align: center; color: var(--muted); font-size: .82rem;
      padding: 24px; border-top: 1px solid var(--border); margin-top: 8px;
    }

    /* ── Toast ── */
    #toast {
      position: fixed; bottom: 24px; right: 24px;
      background: #1e3a5f; color: white;
      padding: 12px 20px; border-radius: 8px;
      font-size: .88rem; box-shadow: 0 4px 16px rgba(0,0,0,.3);
      transform: translateY(80px); opacity: 0; transition: all .3s;
      z-index: 1000; max-width: 320px;
    }
    #toast.show { transform: translateY(0); opacity: 1; }
  </style>
</head>
<body>

<header>
  <div class="logo">🚴</div>
  <div>
    <h1>AdventureWorks Analytics</h1>
    <p>Data Warehouse · Star Schema · ETL Pipeline</p>
  </div>
  <div class="badge">Lab 02</div>
</header>

<main>

  <!-- ETL Banner -->
  <div class="etl-banner">
    <span>⚡ El ETL lee del OLTP y carga el Data Warehouse (OLAP) antes de mostrar resultados.</span>
    <button class="btn-etl" onclick="runETL(this)">▶ Ejecutar ETL</button>
  </div>

  <!-- Q1: Clientes Recurrentes -->
  <div class="card">
    <div class="card-header">
      <div class="card-title-group">
        <div class="card-number">1</div>
        <div class="card-title">Ingresos: Clientes Recurrentes vs No-Recurrentes</div>
        <div class="card-subtitle">Porcentaje de ingresos generados por clientes que han comprado más de una vez</div>
      </div>
      <button class="btn-query" onclick="runQuery('q1', this)">📊 Analizar</button>
    </div>
    <div id="result-q1" class="result-area hidden"></div>
  </div>

  <!-- Q2: Varianza de Margen -->
  <div class="card">
    <div class="card-header">
      <div class="card-title-group">
        <div class="card-number">2</div>
        <div class="card-title">Productos con Mayor Varianza en Margen</div>
        <div class="card-subtitle">Para decisiones de compra de temporada navideña — mayor varianza = mayor riesgo/oportunidad</div>
      </div>
      <button class="btn-query" onclick="runQuery('q2', this)">📈 Analizar</button>
    </div>
    <div id="result-q2" class="result-area hidden"></div>
  </div>

  <!-- Q3: Market Basket -->
  <div class="card">
    <div class="card-header">
      <div class="card-title-group">
        <div class="card-number">3</div>
        <div class="card-title">Top 10 Pares de Productos Comprados Juntos</div>
        <div class="card-subtitle">Análisis de canasta — identifica combos y oportunidades de recomendación</div>
      </div>
      <button class="btn-query" onclick="runQuery('q3', this)">🛒 Analizar</button>
    </div>
    <div id="result-q3" class="result-area hidden"></div>
  </div>

  <!-- Q4: Cohortes -->
  <div class="card">
    <div class="card-header">
      <div class="card-title-group">
        <div class="card-number">4</div>
        <div class="card-title">Análisis de Cohortes y Retención</div>
        <div class="card-subtitle">Top 3 cohortes por margen — retención de clientes por mes desde primera compra</div>
      </div>
      <button class="btn-query" onclick="runQuery('q4', this)">👥 Analizar</button>
    </div>
    <div id="result-q4" class="result-area hidden"></div>
  </div>

</main>

<footer>AdventureWorks ETL · Lab 02 · Data Warehouse con PostgreSQL, SQLAlchemy & Flask</footer>
<div id="toast"></div>

<script>
function showToast(msg, isError = false) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.style.background = isError ? '#c62828' : '#1e3a5f';
  t.classList.add('show');
  setTimeout(() => t.classList.remove('show'), 3000);
}

function setLoading(area) {
  area.innerHTML = '<div class="loading">Consultando el Data Warehouse...</div>';
  area.classList.remove('hidden');
}

async function runQuery(q, btn) {
  const area = document.getElementById('result-' + q);
  btn.disabled = true;
  setLoading(area);
  try {
    const res  = await fetch('/api/query/' + q);
    const data = await res.json();
    if (data.error) throw new Error(data.error);
    area.innerHTML = data.html;
    showToast('✓ Consulta completada');
  } catch(e) {
    area.innerHTML = '<div style="color:#c62828;padding:16px">❌ Error: ' + e.message + '</div>';
    showToast('Error: ' + e.message, true);
  } finally {
    btn.disabled = false;
  }
}

async function runETL(btn) {
  btn.disabled = true;
  btn.textContent = '⏳ Ejecutando...';
  showToast('Iniciando proceso ETL...');
  try {
    const res  = await fetch('/api/run-etl', { method: 'POST' });
    const data = await res.json();
    if (data.success) {
      showToast('✅ ETL completado en ' + data.duration + 's');
    } else {
      showToast('❌ ETL falló: ' + data.error, true);
    }
  } catch(e) {
    showToast('❌ Error: ' + e.message, true);
  } finally {
    btn.disabled = false;
    btn.textContent = '▶ Ejecutar ETL';
  }
}
</script>
</body>
</html>
"""


# ── Query builders ───────────────────────────────────────────────────────────

def q1_html() -> str:
    """P1: Clientes recurrentes vs no recurrentes."""
    rows = query_dw("""
        SELECT
            customer_type,
            SUM(customer_count) AS customer_count,
            SUM(order_count)    AS order_count,
            SUM(total_revenue)  AS total_revenue,
            AVG(revenue_pct)    AS revenue_pct
        FROM dw.agg_customer_recurrence
        GROUP BY customer_type
        ORDER BY total_revenue DESC
    """)
    if not rows:
        return "<p>Sin datos. Ejecuta el ETL primero.</p>"

    total_rev = sum(float(r["total_revenue"]) for r in rows)
    html = '<div class="stats-row">'
    for r in rows:
        pct = float(r["total_revenue"]) / total_rev * 100 if total_rev else 0
        html += f"""
        <div class="stat-box">
          <div class="stat-value">${fmt_number(r['total_revenue'])}</div>
          <div class="stat-label">{r['customer_type']} · {pct:.1f}% del total</div>
        </div>"""
    html += '</div>'

    html += '<table class="result-table"><thead><tr>'
    html += '<th>Tipo de Cliente</th><th class="num">Clientes</th><th class="num">Órdenes</th>'
    html += '<th class="num">Ingresos Totales</th><th class="num">% Ingresos</th>'
    html += '</tr></thead><tbody>'
    for r in rows:
        badge = "badge-recurring" if r["customer_type"] == "Recurring" else "badge-onetime"
        label = "Recurrente" if r["customer_type"] == "Recurring" else "Primera compra"
        html += f"""<tr>
          <td><span class="badge-pill {badge}">{label}</span></td>
          <td class="num">{fmt_number(r['customer_count'])}</td>
          <td class="num">{fmt_number(r['order_count'])}</td>
          <td class="num">${fmt_number(r['total_revenue'])}</td>
          <td class="num">{float(r['revenue_pct'] or 0):.1f}%</td>
        </tr>"""
    html += '</tbody></table>'
    return html


def q2_html() -> str:
    """P2: Productos con mayor varianza en margen."""
    rows = query_dw("""
        SELECT
            dp.product_name,
            dp.category_name,
            dp.subcategory_name,
            dp.price_range,
            COUNT(*)                           AS period_count,
            ROUND(AVG(pm.margin_pct)::numeric, 2) AS avg_margin_pct,
            ROUND(STDDEV(pm.margin_pct)::numeric, 2) AS stddev_margin,
            ROUND(MIN(pm.margin_pct)::numeric, 2) AS min_margin,
            ROUND(MAX(pm.margin_pct)::numeric, 2) AS max_margin,
            SUM(pm.total_revenue)              AS total_revenue
        FROM dw.agg_product_margin pm
        JOIN dw.dim_product dp ON dp.product_key = pm.product_key
        GROUP BY dp.product_key, dp.product_name, dp.category_name,
                 dp.subcategory_name, dp.price_range
        HAVING COUNT(*) > 1
        ORDER BY STDDEV(pm.margin_pct) DESC NULLS LAST
        LIMIT 20
    """)
    if not rows:
        return "<p>Sin datos. Ejecuta el ETL primero.</p>"

    html = '<table class="result-table"><thead><tr>'
    html += '<th>#</th><th>Producto</th><th>Categoría</th><th>Rango</th>'
    html += '<th class="num">Margen Prom %</th><th class="num">Desv. Estándar</th>'
    html += '<th class="num">Min%</th><th class="num">Max%</th><th class="num">Ingresos</th>'
    html += '</tr></thead><tbody>'
    for i, r in enumerate(rows, 1):
        html += f"""<tr>
          <td><span class="badge-pill badge-rank">#{i}</span></td>
          <td><strong>{r['product_name']}</strong></td>
          <td>{r['category_name'] or ''} / {r['subcategory_name'] or ''}</td>
          <td>{r['price_range'] or ''}</td>
          <td class="num">{r['avg_margin_pct']}%</td>
          <td class="num"><strong>{r['stddev_margin']}</strong></td>
          <td class="num">{r['min_margin']}%</td>
          <td class="num">{r['max_margin']}%</td>
          <td class="num">${fmt_number(r['total_revenue'])}</td>
        </tr>"""
    html += '</tbody></table>'
    return html


def q3_html() -> str:
    """P3: Top 10 pares de productos comprados juntos (market basket)."""
    rows = query_dw("""
        SELECT
            da.product_name AS product_a,
            db.product_name AS product_b,
            da.category_name AS cat_a,
            db.category_name AS cat_b,
            mb.co_occurrences,
            mb.support
        FROM dw.agg_market_basket mb
        JOIN dw.dim_product da ON da.product_key = mb.product_key_a
        JOIN dw.dim_product db ON db.product_key = mb.product_key_b
        ORDER BY mb.co_occurrences DESC
        LIMIT 10
    """)
    if not rows:
        return "<p>Sin datos. Ejecuta el ETL primero.</p>"

    html = '<table class="result-table"><thead><tr>'
    html += '<th>#</th><th>Producto A</th><th>Producto B</th>'
    html += '<th class="num">Co-ocurrencias</th><th class="num">Soporte</th>'
    html += '</tr></thead><tbody>'
    for i, r in enumerate(rows, 1):
        html += f"""<tr>
          <td><span class="badge-pill badge-rank">#{i}</span></td>
          <td><strong>{r['product_a']}</strong><br><small style="color:#6b7a8d">{r['cat_a'] or ''}</small></td>
          <td><strong>{r['product_b']}</strong><br><small style="color:#6b7a8d">{r['cat_b'] or ''}</small></td>
          <td class="num"><strong>{r['co_occurrences']}</strong></td>
          <td class="num">{float(r['support'] or 0):.4f}</td>
        </tr>"""
    html += '</tbody></table>'
    return html


def q4_html() -> str:
    """P4: Top 3 cohortes por margen + tabla de retención."""
    # Top 3 cohortes
    top_cohorts = query_dw("""
        SELECT
            cohort_key,
            SUM(total_margin)    AS total_margin,
            SUM(total_revenue)   AS total_revenue,
            MAX(initial_customers) AS initial_customers
        FROM dw.agg_cohort_retention
        GROUP BY cohort_key
        ORDER BY SUM(total_margin) DESC NULLS LAST
        LIMIT 3
    """)

    # Retención de las top 3 cohortes
    retention = query_dw("""
        SELECT
            cr.cohort_key,
            cr.period_number,
            cr.customer_count,
            cr.initial_customers,
            cr.retention_rate,
            cr.total_revenue,
            cr.total_margin,
            cr.avg_revenue_per_customer
        FROM dw.agg_cohort_retention cr
        WHERE cr.cohort_key IN (
            SELECT cohort_key FROM dw.agg_cohort_retention
            GROUP BY cohort_key
            ORDER BY SUM(total_margin) DESC NULLS LAST
            LIMIT 3
        )
        ORDER BY cr.cohort_key, cr.period_number
    """)

    if not top_cohorts:
        return "<p>Sin datos. Ejecuta el ETL primero.</p>"

    # Summary stats de top 3
    html = '<div class="stats-row">'
    for rank, c in enumerate(top_cohorts, 1):
        html += f"""
        <div class="stat-box">
          <div class="stat-value">Cohorte {c['cohort_key']}</div>
          <div class="stat-label">#{rank} · Margen ${fmt_number(c['total_margin'])}</div>
        </div>"""
    html += '</div>'

    # Tabla de retención
    html += '<table class="result-table"><thead><tr>'
    html += '<th>Cohorte</th><th class="num">Periodo</th><th class="num">Clientes Iniciales</th>'
    html += '<th class="num">Activos</th><th class="num">Retención %</th>'
    html += '<th class="num">Ingresos</th><th class="num">Margen</th><th class="num">Ingreso/Cliente</th>'
    html += '</tr></thead><tbody>'

    for r in retention:
        ret_pct = float(r["retention_rate"] or 0) * 100
        color   = "#2d6a4f" if ret_pct >= 50 else ("#f59e0b" if ret_pct >= 25 else "#ef4444")
        html += f"""<tr>
          <td><span class="badge-pill badge-rank">{r['cohort_key']}</span></td>
          <td class="num">+{r['period_number']}m</td>
          <td class="num">{r['initial_customers']}</td>
          <td class="num">{r['customer_count']}</td>
          <td class="num" style="color:{color};font-weight:700">{ret_pct:.1f}%</td>
          <td class="num">${fmt_number(r['total_revenue'])}</td>
          <td class="num">${fmt_number(r['total_margin'])}</td>
          <td class="num">${fmt_number(r['avg_revenue_per_customer'])}</td>
        </tr>"""
    html += '</tbody></table>'
    return html


# ── Flask routes ────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template_string(HTML_TEMPLATE)


@app.route("/api/query/<question>")
def api_query(question: str):
    handlers = {"q1": q1_html, "q2": q2_html, "q3": q3_html, "q4": q4_html}
    if question not in handlers:
        return jsonify({"error": "Pregunta desconocida"}), 404
    try:
        html = handlers[question]()
        return jsonify({"html": html})
    except Exception as e:
        logger.error("Error en query %s: %s", question, e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/run-etl", methods=["POST"])
def api_run_etl():
    """Dispara el ETL como subproceso."""
    import time
    start = time.time()
    try:
        result = subprocess.run(
            [sys.executable, "-m", "src.main"],
            capture_output=True, text=True, timeout=300,
            cwd="/app"
        )
        duration = round(time.time() - start, 1)
        if result.returncode == 0:
            return jsonify({"success": True, "duration": duration})
        else:
            return jsonify({"success": False, "error": result.stderr[-500:]})
    except subprocess.TimeoutExpired:
        return jsonify({"success": False, "error": "ETL tardó más de 5 minutos"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route("/health")
def health():
    try:
        query_dw("SELECT 1")
        return jsonify({"status": "ok", "db": "connected", "timestamp": datetime.now().isoformat()})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 503


@app.route("/api/stats")
def api_stats():
    try:
        stats = {}
        for table in ["dim_customer", "dim_product", "fact_sales", "fact_orders",
                       "agg_market_basket", "agg_cohort_retention"]:
            result = query_dw(f"SELECT COUNT(*) AS n FROM dw.{table}")
            stats[table] = result[0]["n"] if result else 0
        return jsonify({"status": "ok", "counts": stats})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 503


if __name__ == "__main__":
    port = int(os.getenv("FLASK_PORT", 8000))
    app.run(host="0.0.0.0", port=port, debug=False)
