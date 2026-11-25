"""Dashboard de resultados NYC Taxi (Streamlit). Sólo lee lo que ya generó el pipeline.

    streamlit run dashboard/app.py

Fuentes (nada se recalcula fuera de SQL):
- warehouse DuckDB (data/warehouse/nyc_taxi.duckdb o nyc_taxi_dbt.duckdb), abierto en modo lectura;
- sql/q*.sql (las 6 preguntas de la consigna) y sql/insights/i*.sql (insights extra);
- data/reports/quality_AAAA-MM.json y data/quarantine/... (calidad y cuarentena);
- data/control/run_metrics.csv e ingestion_log.csv (rendimiento y trazabilidad).
"""
import json
import sys
from pathlib import Path

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.config import data_dir, literal, monthly_path  # noqa: E402

SQL_DIR = ROOT / "sql"
WAREHOUSES = {"python": "nyc_taxi.duckdb", "dbt": "nyc_taxi_dbt.duckdb"}
PAYMENT = {0: "Flex Fare", 1: "Tarjeta", 2: "Efectivo", 3: "Sin cargo", 4: "Disputa", 5: "Desconocido"}
COLORS = {"comparación": "#8c8c8c", "asignado": "#1f77b4"}

st.set_page_config(page_title="NYC Taxi: resultados", page_icon="🚕", layout="wide")


# ---------------------------------------------------------------- acceso a datos
def warehouse(modelo: str) -> Path:
    return data_dir() / "warehouse" / WAREHOUSES[modelo]


@st.cache_data(ttl=120, show_spinner=False)
def run_sql(modelo: str, sql: str) -> pd.DataFrame:
    # Conexión de sólo lectura y corta: no bloquea al pipeline más que lo que dura la consulta.
    con = duckdb.connect(str(warehouse(modelo)), read_only=True)
    try:
        return con.execute(sql).df()
    finally:
        con.close()


@st.cache_data(ttl=120, show_spinner=False)
def run_file(modelo: str, relative: str) -> tuple[str, pd.DataFrame]:
    sql = (SQL_DIR / relative).read_text(encoding="utf-8")
    return sql, run_sql(modelo, sql)


@st.cache_data(ttl=120, show_spinner=False)
def quality_report(month: str) -> dict | None:
    path = data_dir() / "reports" / f"quality_{month}.json"
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


@st.cache_data(ttl=120, show_spinner=False)
def quarantine_sample(month: str, rule: str, limit: int) -> pd.DataFrame:
    path = monthly_path("quarantine", month, "rejected.parquet")
    if not path.exists():
        return pd.DataFrame()
    con = duckdb.connect()
    try:
        return con.execute(
            f"""SELECT violated_rules, tpep_pickup_datetime, tpep_dropoff_datetime, trip_distance,
                       total_amount, passenger_count, pu_location_id, do_location_id, payment_type, source_file
                FROM read_parquet({literal(path)}, hive_partitioning=false)
                WHERE list_contains(violated_rules, ?) LIMIT {int(limit)}""", [rule]).df()
    finally:
        con.close()


def read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path) if path.exists() else pd.DataFrame()


def show_sql(sql: str):
    with st.expander("Ver la consulta SQL"):
        st.code(sql, language="sql")


def fmt(n) -> str:
    return f"{n:,.0f}".replace(",", ".")


# ---------------------------------------------------------------- barra lateral
st.sidebar.title("🚕 NYC Taxi")
modelo = st.sidebar.radio("Warehouse a leer", list(WAREHOUSES), help="python = src/model.py · dbt = dbt_project/")
if st.sidebar.button("Recargar datos"):
    st.cache_data.clear()
st.sidebar.caption("Monitoreo en vivo del pipeline: [Grafana](http://localhost:3001) · "
                   "Orquestación: [Dagster](http://localhost:3000)")

if not warehouse(modelo).exists():
    st.error(f"No existe {warehouse(modelo)}. Corré primero el pipeline (`.\\run.ps1 pipeline`).")
    st.stop()
try:
    comparison, target = run_sql(modelo, "SELECT comparison_month, target_month FROM analysis_parameters").iloc[0]
except duckdb.IOException:
    st.warning("El warehouse está siendo escrito por el pipeline. Esperá a que termine y tocá «Recargar datos».")
    st.stop()
st.sidebar.markdown(f"**Mes asignado:** `{target}`  \n**Comparado con:** `{comparison}`")

tabs = st.tabs(["Resumen", "Preguntas Q1–Q6", "Insights I1–I7", "Calidad y cuarentena", "Rendimiento"])

# ---------------------------------------------------------------- 1. Resumen
with tabs[0]:
    st.header(f"Resumen {comparison} vs {target}")
    kpi = run_sql(modelo, """SELECT strftime(pickup_date, '%Y-%m') AS month, count(*) AS trips,
                                    avg(fare_amount) AS fare, avg(total_amount) AS total, avg(trip_distance) AS miles
                             FROM fact_trips GROUP BY month ORDER BY month""").set_index("month")
    c, t = kpi.loc[comparison], kpi.loc[target]
    q_c, q_t = quality_report(comparison), quality_report(target)
    cols = st.columns(4)
    cols[0].metric(f"Viajes {target}", fmt(t.trips), f"{100 * (t.trips / c.trips - 1):+.2f} % vs {comparison}")
    cols[1].metric("Tarifa media (fare)", f"US$ {t.fare:.2f}", f"{t.fare - c.fare:+.2f}")
    cols[2].metric("Distancia media", f"{t.miles:.2f} mi", f"{t.miles - c.miles:+.2f}")
    if q_t and q_c:
        pct_t = 100 * q_t["rows_quarantined"] / q_t["rows_read"]
        pct_c = 100 * q_c["rows_quarantined"] / q_c["rows_read"]
        cols[3].metric("Filas en cuarentena", f"{pct_t:.2f} %", f"{pct_t - pct_c:+.2f} pp", delta_color="inverse")
    st.markdown(
        "- **Lectura rápida:** hubo más viajes con tarifa casi igual. La demanda creció sobre todo fuera de Manhattan "
        "(ver I3) y en pagos Flex Fare (ver I2). El análisis completo está en `docs/ANALISIS_RESULTADOS.md`.\n"
        "- Todos los números salen del warehouse; cada pestaña muestra la consulta SQL que los produce."
    )
    daily = run_sql(modelo, """SELECT pickup_date, strftime(pickup_date, '%Y-%m') AS month,
                                      day(pickup_date) AS day, count(*) AS trips
                               FROM fact_trips GROUP BY ALL ORDER BY pickup_date""")
    st.subheader("Viajes por día del mes")
    st.altair_chart(alt.Chart(daily).mark_line(point=True).encode(
        x=alt.X("day:O", title="Día del mes"), y=alt.Y("trips:Q", title="Viajes"),
        color=alt.Color("month:N", title="Mes"), tooltip=["pickup_date:T", "trips:Q"]), use_container_width=True)

# ---------------------------------------------------------------- 2. Preguntas
with tabs[1]:
    st.header("Las seis preguntas de la consigna")
    st.caption("Mismas consultas que generan RESULTADOS.md (sql/q1…q6).")

    sql, df = run_file(modelo, "q1_top_zones.sql")
    st.subheader("Q1. Top 10 zonas de origen y su variación")
    long = df.melt(id_vars="pickup_zone", value_vars=["comparison_trips", "target_trips"], var_name="mes", value_name="viajes")
    long["mes"] = long["mes"].map({"comparison_trips": comparison, "target_trips": target})
    st.altair_chart(alt.Chart(long).mark_bar().encode(
        y=alt.Y("pickup_zone:N", sort="-x", title=None), x=alt.X("viajes:Q"), color="mes:N",
        yOffset="mes:N", tooltip=["pickup_zone", "mes", "viajes"]), use_container_width=True)
    st.dataframe(df, hide_index=True, use_container_width=True)
    show_sql(sql)

    st.subheader("Q2. Hora con mayor ingreso promedio por viaje")
    sql, df = run_file(modelo, "q2_peak_revenue_hour.sql")
    _, by_hour = run_file(modelo, "insights/i7_ingreso_por_minuto.sql")
    peak = int(df.pickup_hour.iloc[0])
    st.metric("Hora pico de ingreso por viaje", f"{peak}:00", f"US$ {df.average_revenue_per_trip.iloc[0]:.2f} por viaje")
    st.altair_chart(alt.Chart(by_hour).mark_bar().encode(
        x=alt.X("pickup_hour:O", title="Hora"), y=alt.Y("average_revenue_per_trip:Q", title="Ingreso promedio por viaje"),
        color=alt.condition(alt.datum.pickup_hour == peak, alt.value("#d62728"), alt.value("#1f77b4")),
        tooltip=["pickup_hour", "trips", "average_revenue_per_trip"]), use_container_width=True)
    show_sql(sql)

    st.subheader("Q3. Viajes con cargo de congestión CBD")
    sql, df = run_file(modelo, "q3_cbd_fee.sql")
    st.metric(f"Viajes con cbd_congestion_fee > 0 en {target}", f"{df.pct_with_cbd_fee.iloc[0]:.2f} %",
              f"{fmt(df.trips_with_cbd_fee.iloc[0])} de {fmt(df.total_trips.iloc[0])}", delta_color="off")
    st.dataframe(df[["pickup_zone", "trips_with_fee", "trips_in_zone", "pct_zone_with_cbd_fee"]], hide_index=True, use_container_width=True)
    st.caption("Rikers Island aparece con 100 % sobre 1 solo viaje: por eso la proporción siempre se lee con su denominador.")
    show_sql(sql)

    st.subheader("Q4. Propina promedio (% de la tarifa) por tipo de pago")
    sql, df = run_file(modelo, "q4_tip_by_payment.sql")
    df["tipo"] = df.payment_type.map(PAYMENT)
    st.altair_chart(alt.Chart(df).mark_bar().encode(
        x=alt.X("tipo:N", sort="-y", title=None), y=alt.Y("average_tip_pct_fare:Q", title="% de la tarifa"),
        tooltip=["tipo", "trips", "average_tip_pct_fare"]), use_container_width=True)
    st.info("El efectivo (tipo 2) no es interpretable: la propina en efectivo no queda registrada en tip_amount, "
            "por eso da casi 0 %. Flex Fare (tipo 0) también sale muy bajo por la misma razón de registro.")
    show_sql(sql)

    st.subheader("Q5. Aeropuertos JFK y LaGuardia")
    sql, df = run_file(modelo, "q5_airports.sql")
    cols = st.columns(len(df))
    for col, row in zip(cols, df.itertuples()):
        col.metric(f"{row.month}", f"{fmt(row.trips)} viajes", f"tarifa media US$ {row.average_fare:.2f}", delta_color="off")
    show_sql(sql)

    st.subheader("Q6. Variación interanual")
    sql, df = run_file(modelo, "q6_month_variation.sql")
    r = df.iloc[0]
    cols = st.columns(2)
    cols[0].metric("Viajes", fmt(r.target_trips), f"{r.trip_variation_pct:+.2f} % ({fmt(r.absolute_trip_variation)})")
    cols[1].metric("Tarifa media", f"US$ {r.target_average_fare:.2f}", f"{r.average_fare_variation:+.2f}")
    st.markdown("**Hipótesis (≤ 5 líneas, en RESULTADOS.md):** más viajes con tarifa casi estable, compatible con más "
                "Flex Fare y con el desvío hacia el taxi tras el peaje CBD. Los insights I2, I3 e I6 la ponen a prueba.")
    show_sql(sql)

# ---------------------------------------------------------------- 3. Insights
with tabs[2]:
    st.header("Insights adicionales")
    st.caption("sql/insights/: consultas extra sobre el mismo modelo estrella. Interpretación completa en docs/ANALISIS_RESULTADOS.md.")

    st.subheader("I1. ¿El peaje CBD aceleró el tránsito en Manhattan?")
    sql, df = run_file(modelo, "insights/i1_velocidad_manhattan_por_hora.sql")
    long = df.melt(id_vars="pickup_hour", value_vars=["comparison_mph", "target_mph"], var_name="mes", value_name="mph")
    long["mes"] = long["mes"].map({"comparison_mph": comparison, "target_mph": target})
    st.altair_chart(alt.Chart(long).mark_line(point=True).encode(
        x=alt.X("pickup_hour:O", title="Hora"), y=alt.Y("mph:Q", title="Velocidad media (mph)", scale=alt.Scale(zero=False)),
        color="mes:N", tooltip=["pickup_hour", "mes", "mph"]), use_container_width=True)
    st.markdown("**Insight:** de noche y a la madrugada Manhattan es 4–7 % más rápido; entre las 10 y las 16 la velocidad "
                "no cambió. La mejora no aparece en las horas de más tránsito.")
    show_sql(sql)

    st.subheader("I2. Mezcla de tipos de pago")
    sql, df = run_file(modelo, "insights/i2_mezcla_tipos_de_pago.sql")
    long = df.melt(id_vars="payment_name", value_vars=["comparison_share_pct", "target_share_pct"], var_name="mes", value_name="pct")
    long["mes"] = long["mes"].map({"comparison_share_pct": comparison, "target_share_pct": target})
    st.altair_chart(alt.Chart(long).mark_bar().encode(
        x=alt.X("payment_name:N", sort=["Flex Fare", "Tarjeta", "Efectivo"], title=None), y=alt.Y("pct:Q", title="% de viajes"),
        color="mes:N", xOffset="mes:N", tooltip=["payment_name", "mes", "pct"]), use_container_width=True)
    st.markdown("**Insight:** Flex Fare pasó de ~12 % a ~28 % de los viajes; tarjeta y efectivo perdieron peso.")
    show_sql(sql)

    st.subheader("I3. Viajes por barrio de origen")
    sql, df = run_file(modelo, "insights/i3_viajes_por_barrio.sql")
    shown = df[~df.pickup_borough.isin(["Unknown", "N/A"])]
    st.altair_chart(alt.Chart(shown).mark_bar().encode(
        x=alt.X("trip_variation_pct:Q", title="Variación de viajes (%)"), y=alt.Y("pickup_borough:N", sort="-x", title=None),
        color=alt.condition(alt.datum.trip_variation_pct > 0, alt.value("#2ca02c"), alt.value("#d62728")),
        tooltip=list(shown.columns)), use_container_width=True)
    st.dataframe(df, hide_index=True, use_container_width=True)
    st.markdown("**Insight:** Brooklyn y el Bronx triplicaron sus viajes; Manhattan creció 12 % pero perdió participación.")
    show_sql(sql)

    st.subheader("I4. Zonas ganadoras y perdedoras")
    sql, df = run_file(modelo, "insights/i4_zonas_ganadoras_y_perdedoras.sql")
    st.altair_chart(alt.Chart(df).mark_bar().encode(
        x=alt.X("trip_change:Q", title="Cambio en viajes"), y=alt.Y("pickup_zone:N", sort="-x", title=None),
        color=alt.Color("grupo:N", scale=alt.Scale(domain=["ganadora", "perdedora"], range=["#2ca02c", "#d62728"])),
        tooltip=list(df.columns)), use_container_width=True)
    if len(df):
        st.caption(f"Sólo {int(df.zones_that_fell.iloc[0])} de {int(df.zones_compared.iloc[0])} zonas con ≥ 1.000 viajes bajaron.")
    show_sql(sql)

    st.subheader(f"I5. Mapa de calor de demanda ({target})")
    sql, df = run_file(modelo, "insights/i5_demanda_dia_hora.sql")
    order = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]
    st.altair_chart(alt.Chart(df).mark_rect().encode(
        x=alt.X("pickup_hour:O", title="Hora"), y=alt.Y("day_name:N", sort=order, title=None),
        color=alt.Color("average_trips:Q", title="Viajes promedio", scale=alt.Scale(scheme="blues")),
        tooltip=["day_name", "pickup_hour", "average_trips"]), use_container_width=True)
    st.markdown("**Insight:** el pico es de martes a jueves entre las 18 y las 22; los fines de semana la demanda nocturna se "
                "extiende hasta la madrugada.")
    show_sql(sql)

    st.subheader("I6. Distribución por distancia")
    sql, df = run_file(modelo, "insights/i6_distribucion_distancias.sql")
    long = df.melt(id_vars="distance_band", value_vars=["comparison_share_pct", "target_share_pct"], var_name="mes", value_name="pct")
    long["mes"] = long["mes"].map({"comparison_share_pct": comparison, "target_share_pct": target})
    st.altair_chart(alt.Chart(long).mark_bar().encode(
        x=alt.X("distance_band:N", title="Millas"), y=alt.Y("pct:Q", title="% de viajes"), color="mes:N", xOffset="mes:N",
        tooltip=["distance_band", "mes", "pct"]), use_container_width=True)
    st.markdown("**Insight:** crecieron los viajes de 3 a 15 millas (típicos desde barrios externos) y bajaron los cortos.")
    show_sql(sql)

    st.subheader("I7. ¿Cuándo rinde más manejar? (ingreso por minuto)")
    sql, df = run_file(modelo, "insights/i7_ingreso_por_minuto.sql")
    st.altair_chart(alt.Chart(df).mark_line(point=True).encode(
        x=alt.X("pickup_hour:O", title="Hora"), y=alt.Y("revenue_per_minute:Q", title="US$ por minuto", scale=alt.Scale(zero=False)),
        tooltip=list(df.columns)), use_container_width=True)
    st.markdown("**Insight:** la madrugada (4–5 h) rinde ~2 US$/min y el mediodía ~1,5 US$/min: el tránsito lento del día "
                "reduce el ingreso por tiempo aunque haya más viajes.")
    show_sql(sql)

# ---------------------------------------------------------------- 4. Calidad
with tabs[3]:
    st.header("Calidad de datos y cuarentena")
    reports = {m: quality_report(m) for m in (comparison, target)}
    if not all(reports.values()):
        st.warning("Faltan reportes de calidad en data/reports/.")
    else:
        cols = st.columns(2)
        for col, (month, rep) in zip(cols, reports.items()):
            ok = rep["rows_read"] == rep["rows_accepted"] + rep["rows_quarantined"]
            col.markdown(f"### {month}")
            col.metric("Leídas", fmt(rep["rows_read"]))
            col.metric("Aceptadas", fmt(rep["rows_accepted"]))
            col.metric("Cuarentena", fmt(rep["rows_quarantined"]), f"{100 * rep['rows_quarantined'] / rep['rows_read']:.2f} %", delta_color="off")
            if ok:
                col.success("Reconciliación: leídas = aceptadas + cuarentena ✔")
            else:
                col.error("Reconciliación fallida")

        rules = pd.DataFrame(reports[target]["rules"])
        viol = pd.DataFrame([{"regla": r, "mes": m, "filas": n} for m, rep in reports.items() for r, n in rep["rule_violations"].items()])
        st.subheader("Filas que violan cada regla")
        st.altair_chart(alt.Chart(viol).mark_bar().encode(
            x=alt.X("regla:N"), y=alt.Y("filas:Q"), color="mes:N", xOffset="mes:N", tooltip=["regla", "mes", "filas"]),
            use_container_width=True)
        st.caption("Una fila puede violar varias reglas. R05 y R08 son advertencias: marcan la fila pero no la descartan.")
        st.dataframe(rules.rename(columns={"id": "regla", "description": "descripción", "severity": "severidad",
                                           "threshold": "umbral", "justification": "justificación"}),
                     hide_index=True, use_container_width=True)

        st.subheader("Explorador de cuarentena")
        st.caption("Para la demo de la consigna: «mostrar una fila en cuarentena con su regla violada».")
        c1, c2, c3 = st.columns([1, 1, 1])
        month = c1.selectbox("Mes", [target, comparison])
        rejecting = [r["id"] for r in reports[target]["rules"] if r["severity"] == "reject"]
        rule = c2.selectbox("Regla", rejecting, index=rejecting.index("R02") if "R02" in rejecting else 0)
        limit = c3.slider("Filas", 5, 100, 20)
        sample = quarantine_sample(month, rule, limit)
        if sample.empty:
            st.info(f"No hay filas en cuarentena por {rule} en {month}.")
        else:
            st.dataframe(sample, hide_index=True, use_container_width=True)

# ---------------------------------------------------------------- 5. Rendimiento
with tabs[4]:
    st.header("Rendimiento y trazabilidad")
    runs = read_csv(data_dir() / "control" / "run_metrics.csv")
    if runs.empty:
        st.info("Todavía no hay corridas medidas. Corré el pipeline y volvé a cargar.")
    else:
        totals = (runs.groupby(["run_id", "started_at", "entrypoint", "modelo"], as_index=False)
                  .agg(segundos=("seconds", "sum"), pico_ram_mb=("peak_rss_mb", "max"),
                       errores=("status", lambda s: int((s != "ok").sum())))
                  .sort_values("started_at", ascending=False))
        last = totals.iloc[0]
        cols = st.columns(3)
        cols[0].metric("Última corrida", f"{last.segundos:.0f} s", f"{last.entrypoint} · {last.modelo}", delta_color="off")
        cols[1].metric("Pico de RAM del proceso", f"{last.pico_ram_mb / 1024:.2f} GB", "límite notebook: 8 GB", delta_color="off")
        cols[2].metric("Corridas medidas", len(totals))
        st.subheader("Duración por etapa (última corrida)")
        stages = runs[runs.run_id == last.run_id].assign(etapa=lambda d: d.stage + " · " + d.period.astype(str))
        st.altair_chart(alt.Chart(stages).mark_bar().encode(
            x=alt.X("seconds:Q", title="Segundos"), y=alt.Y("etapa:N", sort=None, title=None),
            color=alt.Color("peak_rss_mb:Q", title="Pico RAM (MB)", scale=alt.Scale(scheme="orangered")),
            tooltip=["stage", "period", "seconds", "peak_rss_mb", "status"]), use_container_width=True)
        st.subheader("Historial de corridas")
        st.dataframe(totals, hide_index=True, use_container_width=True)
    st.subheader("Registro de ingesta (idempotencia)")
    log = read_csv(data_dir() / "control" / "ingestion_log.csv")
    if not log.empty:
        st.caption("Cada fila es una ingesta: `downloaded` = se bajó, `reused` = ya estaba y el SHA-256 coincidió.")
        st.dataframe(log.tail(20).iloc[::-1], hide_index=True, use_container_width=True)
