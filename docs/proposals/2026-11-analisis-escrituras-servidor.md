# Análisis de las escrituras del servidor — qué hace un commit y cuánto tarda cada parte

**Fecha:** 27 de Noviembre de 2026 · **Código medido:** `ede29c4` · **Datos crudos:** [`tools/perf/results/escrituras/`](../../tools/perf/results/escrituras/)

Sigue a las [evaluaciones de performance](2026-10-evaluaciones-performance.md) y a la [Fase 3.12](2026-11-fase3_12-rendimiento-servidor.md), con el mismo arnés. Aquellas midieron el commit de punta a punta; este análisis lo abre por etapas. La propagación queda afuera: la pregunta es cuántas escrituras por segundo aguanta el servidor.

## 0. Resumen

| Caso | Un commit, 1 en vuelo | Commits/s, 1 en vuelo | Commits/s, 64 en vuelo (una sala, 2 núcleos) | CPU del servidor por commit (64 en vuelo) |
|---|---|---|---|---|
| **1. Ideal** (un hilo, sin disco, red ni runtime) | 1,2 µs (1,4 µs devolviendo la operación) | 863.000 (702.000) | — | 1,2–1,4 µs |
| **2. Disco, sin `fsync`** (el SO escribe cuando quiere) | 131 µs | 7.600 | 28.400 | 62 µs |
| **3. Archivos en memoria** (tmpfs) | 120 µs | 8.300 | 31.500 | 58 µs |
| **4. Disco con `fsync`** (como está hoy) | 542 µs | 1.840 | 20.800 | 67 µs |

- **El `fsync` es el 61% de un commit en disco** (332 µs de 542 en el disco de esta VM), y además encarece el resto: los hilos se duermen mientras esperan y despertarlos cuesta 39 µs más que en tmpfs. Con muchos commits en vuelo el *group commit* lo reparte (25 µs por commit con 64 en vuelo), y el servidor llega al 66% de lo que hace en tmpfs.
- **Sin el `fsync`, el commit no es el disco: es llegar hasta la sala y volver.** De los 87 µs que el servidor tarda en tmpfs, 38 µs (44%) son cuatro traspasos entre hilos y tareas, 39 µs (45%) son HTTP (leer, rutear, autenticar, responder), y solo 8 µs (9%) son el commit en sí: chequear, escribir el registro y la memoria, responder.
- **Los casos 2 y 3 son casi iguales**, y sacar del todo la escritura del archivo no cambia nada (118 µs contra 120): escribir 254 bytes al page cache cuesta 1–1,5 µs.
- **La distancia al ideal es de 80 a 100 veces.** El trabajo propio del commit, medido adentro del servidor (unos 11 µs entre decodificar, chequear, escribir y codificar la respuesta), ya es 8 veces el ideal; el resto es transporte y concurrencia.
- **El servidor entero, con 2 núcleos,** saturó entre 23.000 y 31.000 commits/s en memoria y entre 15.000 y 22.000 con `fsync`, según cómo se repartan los commits entre salas (§4.3).

## 1. Cómo se midió

- **Las sondas.** Un parche de diagnóstico ([`tools/perf/diagnostics/write-stages.patch`](../../tools/perf/diagnostics/write-stages.patch), fuera de `main` como los demás diagnósticos) toma el reloj en 24 puntos del camino de un commit y guarda un histograma por etapa. Las etapas son contiguas: con 1 pedido en vuelo su suma da el tiempo total del servidor con menos de 3% de diferencia (85–87 µs contra 86,9 en tmpfs).
- **Los casos reales** son la Evaluación 1 (`tools/perf/run_stages.sh`): una sala, un cliente al día, inserts de una fila de 5 columnas (184 bytes el pedido, 195 la respuesta, 254 el registro del WAL), HTTP/1.1 por loopback, con 1, 8 y 64 pedidos en vuelo. 3 repeticiones de 15 s tras 3 s de calentamiento; se informa la mediana.
- **El caso ideal** es un modo nuevo del generador (`zemdb-perf ideal`): un hilo que decodifica el frame de un commit, lo chequea, lo guarda en memoria y codifica su confirmación, con las mismas funciones de `zemdb-core` que usa el servidor. 4 millones de commits.
- **El caso 2** usa un flag del parche (`ZEMDB_PERF_NO_FSYNC`) que saltea el `sync_data` de cada commit y los `fsync` de `durable.rs`. La rotación del segmento conserva su propio `sync_all`, así que le quedan 2,3 µs por commit que un modo sin `fsync` de verdad no tendría.
- **El entorno:** una VM de 4 vCPU Intel Xeon a 2,10 GHz, 15 GiB, kernel 6.18.44, Rust 1.97.0; el servidor en los núcleos 0 y 1 y el generador en el 2 y el 3. El disco es `/dev/vda` con ext4.
- **Lo que cuestan las sondas:** un 3% con 1 en vuelo (8.273 commits/s contra 8.532 del servidor sin parche, en tmpfs) y nada medible con 8 y 64. Leer el reloj cuesta unos 30 ns en esta VM.

Qué significa cada número:
- **Los tiempos de las etapas son de reloj, no de CPU.** Los traspasos entre hilos son esperas: con 1 en vuelo pesan enteras, y con el servidor saturado se convierten en colas (por eso con 64 en vuelo `pasar_al_actor` y `volver_al_handler` miden cientos de µs).
- **Con más de 1 en vuelo, el generador infla la respuesta:** todos los pedidos son del mismo cliente con el mismo cursor, así que cada confirmación trae el catch-up de los demás (8 operaciones con 8 en vuelo, 59 con 64: respuestas de 1,2 y 8,6 KiB). Codificarlas cuesta 9 y 15 µs por commit contra 2,7 con 1 en vuelo. Los commits/s con 8 y 64 son entonces un piso.

## 2. El pseudocódigo de cada caso

Tiempos medios en µs, con 1 pedido en vuelo. Cada nivel suma lo de adentro.

### Caso 1 · Ideal

```text
commit()                       1,16 µs   863.000 commits/s en un núcleo
├─ recibir()                   0,50   decode_client_message (184 bytes)
├─ chequeo()                   0,28   cliente, cursor, dedup, esquema, dueño, tamaño
├─ escribir_en_memoria()       0,19   Arc + cola de 1.000 operaciones + ventana de dedup
└─ mandar_confirmacion()       0,29   CommitAck sin catch-up (51 bytes)
                               0,56   si devuelve la operación, como el servidor (195 bytes):
                                      1,42 µs por commit, 702.000 commits/s
```

Las etapas se midieron con una lectura de reloj entre cada una (suman 1,27 µs); el total es de una pasada sin esas lecturas.

### Caso 2 · Disco, sin `fsync` (la sincronización, a cargo del SO)

```text
commit()                                  131 µs   (lo que ve el cliente; 7606 commits/s con 1 en vuelo)
├─ fuera_del_servidor()                   36,7   red local, kernel, despertar del servidor, y el generador
└─ servidor()                             94,1   de la lectura del socket a la escritura de la respuesta
   ├─ recibir()                           20,9
   │  ├─ leer_socket()                    1,3   read()
   │  ├─ parsear_http()                   3,1   hyper: la cabecera
   │  ├─ rutear()                         7,5   axum: router, límites y timeout del cuerpo
   │  ├─ autenticar()                     3,1   sala de la ruta + token (BLAKE3)
   │  ├─ leer_cuerpo()                    3,6
   │  └─ decodificar()                    2,3   decode_client_message + identidad
   ├─ pasar_al_actor()                    13,0   mpsc + despertar la tarea de la sala (salto de hilo)
   ├─ chequear()                          2,0   decide(): registro, cursor, dedup, esquema, dueño, tamaño
   ├─ escribir()                          29,2
   │  ├─ pasar_al_pool_de_E/S()           9,2   semáforo + spawn_blocking (salto de hilo)
   │  ├─ codificar_registro()             1,0   frame del WAL + CRC32 (254 bytes)
   │  ├─ write()                          1,5   seek + write a active.wal
   │  ├─ fsync()                          0,1   sync_data
   │  ├─ escribir_en_memoria()            1,0   búfer en RAM + ventana
   │  ├─ sellar_segmento()                2,3   1 de cada 1.000 commits (2307 µs cada vez)
   │  └─ volver_al_actor()                14,2   despertar al actor (salto de hilo)
   ├─ publicar_y_responder()              2,2   dedup, cabeza, anuncio SSE, lease, catch-up
   ├─ volver_al_handler()                 3,8   oneshot (salto de tarea)
   └─ mandar_confirmacion()               21,3
      ├─ codificar_ack()                  2,9   CommitAck con la operación (195 bytes)
      ├─ armar_respuesta_http()           5,2   axum + hyper
      └─ escribir_socket()                13,2   writev(): en loopback incluye entregar al receptor
```

### Caso 3 · El sistema de archivos en memoria (tmpfs)

```text
commit()                                  120 µs   (lo que ve el cliente; 8273 commits/s con 1 en vuelo)
├─ fuera_del_servidor()                   33,3   red local, kernel, despertar del servidor, y el generador
└─ servidor()                             86,9   de la lectura del socket a la escritura de la respuesta
   ├─ recibir()                           19,3
   │  ├─ leer_socket()                    1,2   read()
   │  ├─ parsear_http()                   2,9   hyper: la cabecera
   │  ├─ rutear()                         7,0   axum: router, límites y timeout del cuerpo
   │  ├─ autenticar()                     2,9   sala de la ruta + token (BLAKE3)
   │  ├─ leer_cuerpo()                    3,3
   │  └─ decodificar()                    2,1   decode_client_message + identidad
   ├─ pasar_al_actor()                    12,4   mpsc + despertar la tarea de la sala (salto de hilo)
   ├─ chequear()                          1,9   decide(): registro, cursor, dedup, esquema, dueño, tamaño
   ├─ escribir()                          26,2
   │  ├─ pasar_al_pool_de_E/S()           9,2   semáforo + spawn_blocking (salto de hilo)
   │  ├─ codificar_registro()             1,0   frame del WAL + CRC32 (254 bytes)
   │  ├─ write()                          1,2   seek + write a active.wal
   │  ├─ fsync()                          0,3   sync_data
   │  ├─ escribir_en_memoria()            1,0   búfer en RAM + ventana
   │  ├─ sellar_segmento()                0,1   1 de cada 1.000 commits (128 µs cada vez)
   │  └─ volver_al_actor()                13,4   despertar al actor (salto de hilo)
   ├─ publicar_y_responder()              2,1   dedup, cabeza, anuncio SSE, lease, catch-up
   ├─ volver_al_handler()                 3,5   oneshot (salto de tarea)
   └─ mandar_confirmacion()               19,6
      ├─ codificar_ack()                  2,7   CommitAck con la operación (195 bytes)
      ├─ armar_respuesta_http()           4,8   axum + hyper
      └─ escribir_socket()                12,1   writev(): en loopback incluye entregar al receptor
```

### Caso 4 · Disco con `fsync` (como está hoy)

```text
commit()                                  542 µs   (lo que ve el cliente; 1842 commits/s con 1 en vuelo)
├─ fuera_del_servidor()                   54,8   red local, kernel, despertar del servidor, y el generador
└─ servidor()                             487   de la lectura del socket a la escritura de la respuesta
   ├─ recibir()                           26,1
   │  ├─ leer_socket()                    1,5   read()
   │  ├─ parsear_http()                   3,9   hyper: la cabecera
   │  ├─ rutear()                         10,0   axum: router, límites y timeout del cuerpo
   │  ├─ autenticar()                     3,9   sala de la ruta + token (BLAKE3)
   │  ├─ leer_cuerpo()                    4,4
   │  └─ decodificar()                    2,5   decode_client_message + identidad
   ├─ pasar_al_actor()                    15,0   mpsc + despertar la tarea de la sala (salto de hilo)
   ├─ chequear()                          2,5   decide(): registro, cursor, dedup, esquema, dueño, tamaño
   ├─ escribir()                          400
   │  ├─ pasar_al_pool_de_E/S()           7,7   semáforo + spawn_blocking (salto de hilo)
   │  ├─ codificar_registro()             1,2   frame del WAL + CRC32 (254 bytes)
   │  ├─ write()                          2,9   seek + write a active.wal
   │  ├─ fsync()                          332   sync_data
   │  ├─ escribir_en_memoria()            1,1   búfer en RAM + ventana
   │  ├─ sellar_segmento()                2,7   1 de cada 1.000 commits (2624 µs cada vez)
   │  └─ volver_al_actor()                52,4   despertar al actor (salto de hilo)
   ├─ publicar_y_responder()              3,3   dedup, cabeza, anuncio SSE, lease, catch-up
   ├─ volver_al_handler()                 6,6   oneshot (salto de tarea)
   └─ mandar_confirmacion()               31,6
      ├─ codificar_ack()                  4,7   CommitAck con la operación (195 bytes)
      ├─ armar_respuesta_http()           8,3   axum + hyper
      └─ escribir_socket()                18,6   writev(): en loopback incluye entregar al receptor
```

- **El `fsync`:** mediana 311 µs, p99 819 µs en este disco. Con 64 en vuelo cada uno tarda más (media 639 µs) porque sincroniza un grupo de 26 commits.
- **Todo lo demás es más lento que en tmpfs** (155 µs contra 87 sin contar el `fsync`): el servidor pasa el 60% del tiempo esperando al disco, los núcleos entran en reposo, y cada etapa arranca en frío. Lo que más crece es `volver_al_actor`, de 13 a 52 µs.

## 3. Dónde se va el tiempo

El servidor en tmpfs, 1 en vuelo (87 µs):

| Grupo | µs | % | Qué es |
|---|---|---|---|
| **Traspasos entre hilos y tareas** | 38,4 | 44% | al actor 12,4 · al pool de E/S 9,2 · de vuelta al actor 13,4 · de vuelta al handler 3,5 |
| **HTTP de entrada** | 19,3 | 22% | leer, parsear, rutear (7,0), autenticar, cuerpo, decodificar |
| **HTTP de salida** | 19,6 | 23% | codificar, armar la respuesta, escribir el socket (12,1) |
| **El commit en sí** | 7,6 | 9% | chequear 1,9 · registro + `write` 2,2 · memoria 1,0 · publicar y responder 2,1 |

Tres variantes de diagnóstico separan esos grupos (commits/s con 1 en vuelo):

| Variante | Commits/s | Un commit | Qué saca |
|---|---|---|---|
| tmpfs, como está | 8.273 | 120 µs | — |
| tmpfs, E/S en el hilo del actor | 9.822 | 101 µs | el pool de E/S: 19 µs |
| sin WAL, E/S en el hilo del actor | 10.272 | 97 µs | además, el archivo: 4 µs |
| el actor sin HTTP, en tmpfs | 19.217 | 51 µs | HTTP y la red: 69 µs |
| el actor sin HTTP, sin WAL, sin pool | 78.359 | 12 µs | todo salvo el actor y su mailbox |

- **El actor solo, sin transporte ni pool, hace un commit en 12 µs.** Es el piso de la arquitectura actual (una tarea por sala, un `mpsc` y un `oneshot` por commit), 9 veces el ideal.
- **HTTP y la red local cuestan 69 µs por commit**, más que todo el resto junto. De ellos, 33 µs están fuera del servidor (kernel, loopback y el generador), y el resto es el HTTP de adentro del servidor.
- **El pool de E/S cuesta 19 µs por commit con 1 en vuelo** (los 27 µs que midió la Fase 3.12 en otra VM). Existe para que un `fsync` no frene a las otras salas; cuando no hay `fsync` que esperar es puro costo.

## 4. Escrituras por segundo

### 4.1 Una sala, medido

| Caso | 1 en vuelo | 8 en vuelo | 64 en vuelo | CPU por commit (1 / 8 / 64) |
|---|---|---|---|---|
| 2. Disco sin `fsync` | 7.606 | 25.664 | 28.359 | 121 / 66 / 62 µs |
| 3. tmpfs | 8.273 | 27.528 | 31.500 | 115 / 65 / 58 µs |
| 4. Disco con `fsync` | 1.842 | 6.919 | 20.841 | 197 / 110 / 67 µs |
| Sin WAL (diagnóstico) | 8.416 | 28.718 | 31.184 | 111 / 62 / 59 µs |

Latencia del cliente (media / p99, en µs): caso 2, 131 / 257 con 1 en vuelo y 2.256 / 5.422 con 64; caso 3, 120 / 235 y 2.031 / 4.104; caso 4, 542 / 1.361 y 3.070 / 7.004.

### 4.2 El modelo

- **Con 1 en vuelo,** commits/s = 1 / (lo que tarda un commit): `1 / 120 µs = 8.300` en tmpfs, `1 / 542 µs = 1.840` en disco. Para otro disco, con `f` el tiempo de su `fsync` en µs: **commits/s ≈ 1.000.000 / (210 + f)**. Con un `fsync` de 1 ms (un disco de red típico), unos 830; con uno de 50 µs, unos 3.800.
- **Sin `fsync` (casos 2 y 3), el techo es la CPU.** Saturado, el servidor gasta 58–62 µs de CPU por commit, es decir **unos 17.000 commits/s por núcleo** (medido: 31.500 con 2 núcleos al 184%). Con el catch-up de un solo commit en la respuesta, en lugar de las 59 operaciones que manda el generador, la cuenta de las etapas da unos 43 µs: del orden de 23.000 por núcleo. Esto último es una estimación, no una medición.
- **Con `fsync`, el techo de una sala es el ciclo de su actor:** un grupo por `fsync`. Con 64 en vuelo cada ciclo dura 1,24 ms (639 µs el `fsync`, 270 µs ir y volver del pool con el servidor cargado, el resto secuenciar y responder) y lleva 26 commits: `26 / 1,24 ms = 20.800`. El grupo no llega a 64 porque la mitad de los pedidos está en viaje mientras el actor espera al disco.

### 4.3 El servidor entero, varias salas a la vez

Una corrida de 12 s por configuración, con un generador por sala compartiendo 2 núcleos (los generadores quedaron cerca de su límite, así que son pisos):

| Salas × pedidos en vuelo | tmpfs: commits/s (CPU) | Disco con `fsync`: commits/s (CPU) |
|---|---|---|
| 4 × 16 | 25.438 (184%) | 22.218 (145%) |
| 16 × 4 | 28.273 (186%) | 16.449 (153%) |
| 32 × 1 | 23.373 (159%) | 15.015 (155%) |

- **En memoria el total no depende de cuántas salas haya:** lo fija la CPU, entre 23.000 y 31.000 commits/s con 2 núcleos.
- **Con `fsync`, repartir la misma carga en más salas baja el total** (de 22.000 a 15.000): cada sala arma grupos más chicos y hay más `fsync` por commit. Los `fsync` de salas distintas corren en paralelo en el pool, pero comparten el mismo disco.

### 4.4 La escalera

| | Commits/s | Contra el escalón anterior |
|---|---|---|
| Ideal, un núcleo | 702.000 | — |
| El actor solo, 1 en vuelo | 78.000 | ÷ 9: tokio, el mailbox y la respuesta |
| + WAL en tmpfs y pool de E/S | 19.200 | ÷ 4: dos saltos de hilo por commit |
| + HTTP por loopback (caso 3) | 8.300 | ÷ 2,3 |
| + disco sin `fsync` (caso 2) | 7.600 | ÷ 1,1 |
| + `fsync` (caso 4) | 1.840 | ÷ 4,1 |
| Caso 4 con 64 en vuelo | 20.800 | × 11: el *group commit* |

## 5. Lo que deja ver, sin optimizar nada todavía

En orden de cuánto pesa, para el día que se decida optimizar (medir no es optimizar, DP2):

1. **El `fsync` por commit** domina el caso real con poca concurrencia. El *group commit* ya lo resuelve con carga; con 1 en vuelo no hay con quién agrupar. Un modo de durabilidad relajada (el caso 2) multiplicaría por 4 los commits/s con 1 en vuelo, a cambio de perder los últimos commits confirmados en un corte de energía.
2. **Los traspasos de hilo** son el 44% del servidor cuando no hay `fsync`. El del pool de E/S (19 µs) es el más fácil de evitar donde la escritura no bloquea; quedó anotado en §6 de las evaluaciones.
3. **El ruteo de axum** cuesta 7 µs, más que autenticar, leer el cuerpo y decodificar juntos. Incluye el middleware del límite de tiempo del cuerpo.
4. **`escribir_socket`** (12 µs) es del kernel y en loopback incluye entregarle los bytes al receptor. Con una red real cambia de forma: no conviene sacar conclusiones de este número.
5. **El commit en sí** (8 µs) es 5 veces el ideal y no es prioridad mientras lo de arriba siga ahí.

## 6. Límites de la medición

- **El disco es el de una VM**, con un `fsync` de 0,3 ms. Un NVMe local o un disco de red dan otros números para el caso 4; por eso el modelo de §4.2 deja `f` como parámetro.
- **Cliente y servidor comparten máquina**, y el servidor tuvo 2 núcleos. Los 33 µs de "fuera del servidor" incluyen al generador.
- **Una sola forma de carga:** inserts chicos, un cliente por sala, sin streams SSE abiertos. Con suscriptores, `publicar` arma además el evento `ops`.
- **§4.3 es una corrida por configuración,** sin repeticiones, y con el generador como posible límite.
- **Las VMs de este entorno cambian entre sesiones:** los números absolutos no se comparan con los de la Fase 3.12 (DP3).

## 7. Cómo repetirlo

```bash
git apply tools/perf/diagnostics/write-stages.patch
cargo build --release -p zemdb-server && (cd tools/perf && cargo build --release)
tools/perf/target/release/zemdb-perf ideal --n 4000000 --echo 1
for caso in nofsync tmpfs disk; do tools/perf/run_stages.sh $caso salida.jsonl; done
tools/perf/stages.py salida.jsonl
git checkout crates/server tools/perf/src/main.rs
```

`run_stages.sh` acepta también `nowal`, `inproc-tmpfs`, `inproc-nowal` y el sufijo `+inline` (la E/S en el hilo del actor). El modo `ideal` no necesita el parche.
