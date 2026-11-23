"""Formateo e interpretaciones neutrales de las seis consultas de negocio."""
from collections.abc import Sequence


def markdown_table(headers, rows):
    def cell(value):
        if value is None:
            return "NULL"
        if isinstance(value, float):
            return f"{value:.4f}"
        return str(value).replace("|", "\\|")

    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join("---" for _ in headers) + " |"]
    lines.extend("| " + " | ".join(cell(value) for value in row) + " |" for row in rows)
    return "\n".join(lines)


def _value(headers: Sequence[str], row: Sequence, name: str):
    try:
        return row[list(headers).index(name)]
    except (ValueError, IndexError):
        return None


def _number(value):
    if value is None:
        return "NULL"
    if isinstance(value, float):
        return f"{value:.4f}"
    return f"{value:,}".replace(",", ".") if isinstance(value, int) else str(value)


def interpret(query_name: str, headers: Sequence[str], rows: list[tuple], months: list[str]) -> str:
    """Devuelve una interpretación basada sólo en las filas de la consulta."""
    comparison, target = months
    if not rows:
        return f"No se obtuvieron filas para {comparison} y {target}; no se formula una conclusión."

    first = rows[0]
    if query_name == "q1_top_zones.sql":
        zone = _value(headers, first, "pickup_zone")
        trips = _value(headers, first, "target_trips")
        variation = _value(headers, first, "variation_pct")
        return f"El ranking muestra las zonas de origen con más viajes en {target}; la primera es {zone} con {_number(trips)} viajes y variación {_number(variation)} % frente a {comparison}."
    if query_name == "q2_peak_revenue_hour.sql":
        hour = _value(headers, first, "pickup_hour")
        average = _value(headers, first, "average_revenue_per_trip")
        return f"La hora {hour}:00 presenta el mayor ingreso promedio por viaje en {target} ({_number(average)}). La consulta no permite atribuir causalidad ni separar aeropuertos sin análisis adicional."
    if query_name == "q3_cbd_fee.sql":
        total = _value(headers, first, "total_trips")
        with_fee = _value(headers, first, "trips_with_cbd_fee")
        pct = _value(headers, first, "pct_with_cbd_fee")
        zone = _value(headers, first, "pickup_zone")
        zone_pct = _value(headers, first, "pct_zone_with_cbd_fee")
        return f"En {target}, {with_fee} de {total} viajes registran cbd_congestion_fee > 0 ({_number(pct)} %). La zona superior por proporción es {zone} ({_number(zone_pct)} %); siempre debe leerse junto con su denominador."
    if query_name == "q4_tip_by_payment.sql":
        return "La consulta compara la propina registrada como porcentaje de tarifa por tipo de pago. Las propinas en efectivo no se registran completamente en tip_amount, por lo que ese grupo no mide toda la propina real."
    if query_name == "q5_airports.sql":
        summary = "; ".join(
            f"{_value(headers, row, 'month')}: {_number(_value(headers, row, 'trips'))} viajes, tarifa media {_number(_value(headers, row, 'average_fare'))}"
            for row in rows
        )
        return f"Se identifican viajes con origen o destino en JFK o LaGuardia para los períodos comparados: {summary}."
    if query_name == "q6_month_variation.sql":
        trip_pct = _value(headers, first, "trip_variation_pct")
        fare_delta = _value(headers, first, "average_fare_variation")
        return (
            f"Entre {comparison} y {target}, la variación porcentual de viajes es {_number(trip_pct)} % "
            f"y la variación de tarifa media es {_number(fare_delta)}. "
            "Hipótesis de causa: el aumento de volumen con tarifa media casi estable es compatible con una mayor "
            "proporción de viajes Flex Fare (payment_type = 0) y de trayectos urbanos cortos e intermedios, "
            "sumado al desvío de viajes hacia el taxi tras la entrada en vigor del peaje CBD en 2025. "
            "Es una hipótesis, no una conclusión: dos cortes mensuales no la confirman sin controlar "
            "estacionalidad plurianual, oferta de vehículos y otros modos de transporte."
        )
    return f"Resultado calculado para {comparison} y {target}."


def render_results(output_path, months: list[str], engine: str, results) -> None:
    comparison, target = months
    output = [
        "# Resultados del modelo NYC Taxi",
        "",
        f"Motor: `{engine}`; mes comparado: `{comparison}`; mes asignado: `{target}`.",
        "",
    ]
    for name, query, description, rows in results:
        headers = [item[0] for item in description]
        output.extend(
            [
                f"## {name}",
                "",
                "Consulta:",
                "```sql",
                query,
                "```",
                "",
                markdown_table(headers, rows),
                "",
                interpret(name, headers, rows, months),
                "",
            ]
        )
    output_path.write_text("\n".join(output), encoding="utf-8")
