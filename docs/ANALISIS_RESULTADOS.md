# Análisis de resultados e insights: junio 2025 vs junio 2024

Este documento interpreta los números que produce el pipeline. Cada afirmación sale de una consulta versionada:

- `RESULTADOS.md`, que generan las seis preguntas de la consigna (`sql/q1…q6`);
- `INSIGHTS.md`, que generan siete consultas extra (`sql/insights/i1…i7`) con `python -m src.insights`;
- los reportes de calidad, en `data/reports/quality_AAAA-MM.json`.

Los gráficos de todo esto están en el dashboard (`.\run.ps1 dashboard`).

> **Cómo leerlo.** Cada hallazgo tiene cuatro partes: **qué vemos** (el número), **qué significa**, **qué lo respalda** (otra consulta) y **cuidado** (por qué no es una conclusión definitiva). Con dos meses no se puede probar una causa; sí se pueden descartar explicaciones y proponer hipótesis.

---

## 1. Resumen ejecutivo (5 ideas)

1. **Hubo 17,75 % más viajes con la misma tarifa media.** Los viajes válidos pasaron de 3.441.465 a 4.052.233 (+610.768). La tarifa media (`fare_amount`) bajó apenas US$ 0,16, de 19,87 a 19,71.
2. **La demanda creció sobre todo fuera de Manhattan.** Brooklyn (+227 %) y el Bronx (+228 %) triplicaron sus viajes; Manhattan creció 12 % y bajó del 88,8 % al 84,6 % del total (I3).
3. **El medio de pago cambió.** Flex Fare (`payment_type = 0`) pasó del 11,6 % al 27,6 % de los viajes, mientras tarjeta y efectivo perdieron peso (I2).
4. **La tarifa "estable" esconde dos movimientos opuestos.** Hay más viajes largos, que suben el promedio, pero cada tramo de distancia es más barato, lo que lo baja (I6). Es un efecto de composición.
5. **La calidad de los datos empeoró en 2025.** La cuarentena pasó del 2,76 % al 6,26 % de las filas, por casi 68.000 viajes con subida y bajada en el mismo segundo (regla R02).

---

## 2. Las seis preguntas de la consigna, interpretadas

### Q1. Top 10 zonas de origen

**Qué vemos.**
- JFK Airport encabeza 2025-06 con 170.662 viajes (+12,15 %).
- Le siguen Upper East Side South (+8,51 %) y Midtown Center (+5,51 %).
- Nueve de las diez zonas crecieron. La única que cayó es **LaGuardia Airport (−3,55 %)**.

**Qué significa.** El núcleo de la demanda sigue siendo el mismo: aeropuertos y Manhattan entre la 14 y la 96. El crecimiento total no viene de nuevas zonas "estrella", sino de que casi todas suben.

**Qué lo respalda.** I4: de las 90 zonas con al menos 1.000 viajes en 2024, **sólo 3 bajaron** (East Elmhurst, Central Park y LaGuardia).

**Cuidado.** La caída de LaGuardia contrasta con la suba de JFK. Puede deberse a obras de acceso o a cambios de aerolíneas, cosas que el dataset no muestra. No lo atribuimos a nada.

### Q2. Hora de mayor ingreso promedio por viaje

**Qué vemos.** Es a las **5:00**, con US$ 32,57 por viaje.

**Qué significa.** A esa hora hay pocos viajes, pero son más largos y con más peso de aeropuertos.

**Qué lo respalda.**
- A las 5:00, el **16,1 %** de los viajes empieza o termina en JFK o LaGuardia, contra el **8,6 %** en todo el mes. Los viajes de aeropuerto tienen tarifas mucho más altas (Q5).
- I7 muestra que la madrugada también es la franja de mayor **ingreso por minuto** (~US$ 2,0/min a las 4–5 h, contra ~1,5 a mediodía). El tránsito libre permite cobrar distancia sin perder tiempo.

**Cuidado.** "Mayor ingreso por viaje" no es "mayor ingreso total". El ingreso total es máximo entre las 17 y las 18, porque hay muchos más viajes.

### Q3. Cargo de congestión CBD

**Qué vemos.**
- El **73,73 %** de los viajes de 2025-06 (2.987.770 de 4.052.233) pagó `cbd_congestion_fee > 0`.
- Las zonas con mayor proporción son las del sur de Manhattan (Greenwich Village, West Village, SoHo), con más del 99,7 %.

**Qué significa.** El peaje de congestión, vigente desde el 5 de enero de 2025, alcanza a casi tres de cada cuatro viajes en taxi amarillo, porque la mayoría entra, sale o circula por el sur de la calle 60.

**Cuidado (denominador).** Rikers Island figura con 100 %, pero sobre **1 solo viaje**. Por eso la consulta devuelve también el número de viajes de cada zona: una proporción sin denominador engaña.

### Q4. Propina por tipo de pago

**Qué vemos.**

| Tipo de pago | Propina (% de la tarifa) |
| --- | --- |
| Tarjeta (1) | 25,26 % |
| Flex Fare (0) | 1,49 % |
| Efectivo (2) | 0,002 % |

**Qué significa.** El **efectivo no es interpretable**: la propina en efectivo se da en mano y no queda registrada en `tip_amount`, así que el ~0 % no significa que no se deje propina. Flex Fare tampoco: es una tarifa acordada por adelantado y la propina casi no aparece en el registro.

**Cuidado.** Sólo la tarjeta mide propina real, y aun así incluye las propinas sugeridas por la terminal (20/25/30 %).

### Q5. Aeropuertos JFK y LaGuardia

**Qué vemos.**

| Mes | Viajes | Tarifa media |
| --- | --- | --- |
| 2024-06 | 334.938 | US$ 56,26 |
| 2025-06 | 346.769 | US$ 57,76 |

**Qué significa.** Los aeropuertos crecieron mucho menos que el total: **+3,5 % contra +17,75 %**. Su participación cae: el crecimiento de 2025 es urbano, no aeroportuario.

**Cuidado.** Q5 junta los dos aeropuertos. Q1 e I4 muestran que JFK sube y LaGuardia baja.

### Q6. Variación interanual y su hipótesis

**Qué vemos.** **+17,75 % de viajes** (+610.768) y **−US$ 0,16 de tarifa media**.

**Hipótesis** (la misma que está en `RESULTADOS.md`): más volumen con tarifa casi igual, compatible con más viajes Flex Fare y con desvío de demanda hacia el taxi tras el peaje CBD.

**Qué la respalda y qué la matiza:**
- **A favor:**
  - I2: Flex Fare suma +718.000 viajes y +16 puntos de participación. Es el componente que más creció.
  - I3: el crecimiento fuerte está en Brooklyn y el Bronx, donde el taxi amarillo tenía poca presencia.
  - I6: tarifas más bajas por tramo de distancia.
- **Matiz, efecto calendario:**
  - Junio 2024 empezó un sábado y tuvo 10 días de fin de semana (5 sábados y 5 domingos). Junio 2025 empezó un domingo y tuvo 9.
  - El fin de semana tiene menos viajes por día, así que 2025 tiene un día hábil más.
  - La proporción de viajes de fin de semana bajó de 31,6 % a 30,0 %.
  - Un día hábil pesa alrededor de 3 % del mes: explica una parte pequeña del +17,75 %, no el total.
- **En contra de una lectura simple:** casi 4 de cada 10 viajes nuevos (236.418 de 610.768) salen de fuera de Manhattan, donde no se cobra el peaje. Así que el peaje no puede ser la única causa.

**Cuidado.** Dos meses no alcanzan para separar tendencia, estacionalidad y política. Para confirmarlo habría que correr el pipeline para varios meses: el diseño ya lo permite con `--mes`.

---

## 3. Insights adicionales (I1–I7)

### I1. ¿El peaje hizo que Manhattan ande más rápido?

**Qué vemos.** Comparamos la velocidad media de los viajes internos de Manhattan:

| Franja | 2024 | 2025 | Variación |
| --- | --- | --- | --- |
| 0 h | 11,39 mph | 12,17 mph | +6,9 % |
| 23 h | 10,97 mph | 11,71 mph | +6,7 % |
| 10 a 16 h | — | — | entre −0,2 % y +0,6 % (sin cambio) |

**Qué significa.** La mejora aparece **de noche y temprano**, no en las horas de más tránsito. A mediodía Manhattan sigue a ~8,2 mph en ambos años.

**Cuidado.**
- El peaje es más caro de día, así que sería esperable ver el efecto justo al revés. Una explicación posible es que a mediodía la calle esté saturada por obras, delivery y buses, y que el peaje no alcance a mover la aguja.
- Es una observación, no una evaluación del peaje: faltan otros meses y un grupo de control.
- El promedio excluye velocidades imposibles (< 0,5 o > 80 mph).

### I2. Mezcla de tipos de pago

**Qué vemos.**
- **Flex Fare:** 11,6 % → 27,6 % (+16,0 pp).
- **Tarjeta:** 74,0 % → 62,1 % (−11,9 pp).
- **Efectivo:** 13,0 % → 8,7 % (−4,3 pp).

**Qué significa.** Es el cambio estructural más grande entre los dos meses. Los viajes Flex Fare de 2025 son **más largos** (3,88 mi contra 3,51 del resto) pero con **tarifa media más baja** (US$ 18,51 contra 20,17): es una tarifa acordada de antemano.

**Por qué importa para el pipeline.** El 100 % de los 1.118.536 viajes Flex Fare aceptados de 2025 tiene `passenger_count` nulo. Si la regla R05 (pasajeros nulos) fuera de **rechazo**, habríamos descartado el componente que más creció y la Q6 daría una variación falsa. Por eso R05 es una **advertencia**: es la decisión de severidad que mejor se justifica con datos.

### I3. Viajes por barrio

| Barrio | Viajes 2024-06 | Viajes 2025-06 | Variación | Participación 2025 |
| --- | --- | --- | --- | --- |
| Manhattan | 3.054.777 | 3.429.127 | +12,25 % | 84,62 % |
| Queens | 310.422 | 404.268 | +30,23 % | 9,98 % |
| Brooklyn | 54.309 | 177.507 | +226,85 % | 4,38 % |
| Bronx | 10.343 | 33.931 | +228,06 % | 0,84 % |

**Qué significa.** El taxi amarillo, históricamente un servicio de Manhattan, ganó presencia en los barrios externos. El ingreso de Brooklyn pasó de US$ 1,74 M a US$ 5,03 M.

**Cuidado.** Son crecimientos grandes sobre bases chicas. Además, no sabemos si el aumento viene de viajes por app (por ejemplo, Flex Fare pedido desde una aplicación), porque el dataset no tiene el canal de solicitud.

### I4. Zonas ganadoras y perdedoras

**Ganadoras** (en viajes absolutos):
- East Village: +23.147 (+28 %);
- JFK: +18.491;
- Upper East Side South: +13.317;
- Penn Station: +13.255;
- Lower East Side: +12.483 (+31 %).

**Perdedoras:** sólo 3 de 90 zonas:
- LaGuardia: −4.013;
- Central Park: −3.118;
- East Elmhurst: −1.165.

**Qué significa.** El crecimiento es generalizado. Las zonas de vida nocturna (East Village, Lower East Side) crecieron más del 28 %, lo que coincide con la mejora de velocidad nocturna de I1.

**Cuidado.** Se excluyen las zonas 264/265 ("Unknown" / "N/A"): son códigos de desconocido, no lugares.

### I5. Mapa de calor día × hora

**Qué vemos.**
- El pico absoluto es el **miércoles a las 21 h** (~10.200 viajes por hora en promedio), seguido por martes y jueves a la noche.
- El fin de semana la demanda se corre a la madrugada: de 0 a 2 h hay **~8.000 viajes por hora sábados y domingos, contra ~2.000 en días hábiles**.

**Cuidado.** Se promedia por fecha porque junio 2025 tiene 5 domingos y 5 lunes pero 4 del resto de los días. Si se sumara, esos dos días parecerían más fuertes.

### I6. Distribución por distancia

| Tramo (millas) | % 2024 | % 2025 | Tarifa media 2024 | Tarifa media 2025 |
| --- | --- | --- | --- | --- |
| menos de 1 | 21,20 | 19,29 | 8,28 | 8,67 |
| 1 a 3 | 49,28 | 46,71 | 13,74 | 13,27 |
| 3 a 7 | 16,82 | 20,07 | 25,12 | **23,08** |
| 7 a 15 | 8,06 | 9,63 | 44,61 | **40,66** |
| 15 o más | 4,64 | 4,31 | 75,86 | 76,40 |

**Qué significa.** Este es el insight más fino del análisis: **la tarifa media no cambió, pero no porque nada haya cambiado**.
- Hay más viajes de 3 a 15 millas, típicos desde Brooklyn y Queens. Eso **sube** el promedio.
- Pero dentro de esos mismos tramos la tarifa bajó entre 8 y 9 %, compatible con Flex Fare. Eso lo **baja**.
- Los dos efectos se compensan.

Es un caso de efecto de composición: un promedio agregado puede ocultar cambios en sentidos opuestos.

### I7. ¿Cuándo rinde más manejar?

**Qué vemos.**
- **De madrugada:** el ingreso por minuto con pasajero es máximo a las 4 y 5 h (~US$ 2,0/min).
- **A mediodía:** es mínimo a las 13 y 15 h (~US$ 1,43–1,47/min), porque los viajes duran más por el tránsito (19–20 minutos contra 15).

**Qué significa.** Complementa a la Q2. Para un taxista, la tarde-noche (18 a 23 h) combina mucho volumen con un ingreso por minuto alto (~1,75), así que es la franja más conveniente para trabajar.

**Cuidado.** Sólo cuenta el tiempo con pasajero. El tiempo buscando viaje, que no está en el dataset, cambia la conclusión de "rendimiento real".

---

## 4. Lo que dicen los datos sobre la calidad de los datos

| Regla | 2024-06 | 2025-06 | Comentario |
| --- | --- | --- | --- |
| R01 subida fuera del mes | 51 | 20 | Marginal |
| **R02 bajada ≤ subida** | **1.140** | **68.583** | **×60** |
| R03 distancia ≤ 0 o > 200 mi | 53.084 | 135.118 | ×2,5 |
| R04 total negativo | 48.795 | 74.257 | Reversos de cobro |
| R05 pasajeros nulos (advertencia) | 446.620 | 1.235.737 | Flex Fare sin pasajeros |
| R06 zona inexistente | 0 | 0 | Integridad referencial OK |
| R07 duplicados exactos | 0 | 0 | — |
| R08 propina negativa (advertencia) | 108 | 367 | Reversos |
| **Filas en cuarentena** | **97.728 (2,76 %)** | **270.727 (6,26 %)** | +3,5 pp |

**El hallazgo de R02.**
- De las 68.583 filas de 2025 con bajada no posterior a la subida, **68.352 (99,7 %) tienen exactamente el mismo segundo** de subida y de bajada.
- La mayoría está pagada con tarjeta.
- Parece un problema sistemático de registro (por ejemplo, viajes cancelados o cobrados sin trayecto) que no existía en 2024.

Es justo lo que la consigna anticipa: *"la TLC no garantiza la exactitud; encontrar y tratar esa suciedad es parte del examen"*. Gracias a la cuarentena esas filas no se pierden: se pueden auditar desde el dashboard (pestaña "Calidad y cuarentena").

**Implicancia para el análisis.**
- La cuarentena crece en 2025, así que la variación de viajes **válidos** (+17,75 %) es menor que la de viajes **crudos** (+22,1 %: de 3.539.193 a 4.322.960).
- Elegimos comparar viajes válidos porque es lo que se puede defender. Mezclar filas con duración cero en promedios de tarifa o velocidad los distorsionaría.

---

## 5. Limitaciones del análisis

- **Dos meses no alcanzan para hablar de causas.** Para separar tendencia, estacionalidad y política habría que procesar todos los meses (el pipeline ya lo soporta con `--mes`).
- **Faltan variables de contexto:** clima, eventos, oferta de vehículos, precios de apps (Uber/Lyft) y transporte público.
- **`payment_type` mezcla medio de pago y modalidad tarifaria** (Flex Fare). La interpretación de propinas sólo es válida para tarjeta.
- **Los promedios de velocidad e ingreso por minuto excluyen valores físicamente imposibles**, sólo en esas consultas. Las reglas de calidad generales no cambian.
