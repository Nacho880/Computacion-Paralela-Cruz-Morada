# Trabajo Paralelo — Cruz Morada

**Curso:** Computación Paralela y Distribuida  
**Institución:** Universidad Tecnológica Metropolitana (UTEM)  
**Entrega:** 15 de junio de 2026  
**Integrantes:**
- [Matias Fernandez  20.969.062-4]
- [Camilo Moya       21.230.348-8]
- [Ignacio Ortega    21.481.176-6]

**Repositorio:** [URL del repositorio]

---

## Descripción

Solución en C++ con OpenMP que procesa archivos de ventas de la cadena farmacéutica Cruz Morada. El programa descarga reportes CSV desde un servidor SFTP, consulta una API REST para obtener el género de cada cliente, y calcula el promedio de compras por género (MASCULINO/FEMENINO) usando procesamiento paralelo.

---

## Requisitos

- Ubuntu 24.04 LTS
- GCC con soporte OpenMP (`gcc`, `g++`)
- `libcurl4-openssl-dev`
- `libssh2-1-dev`
- `libssl-dev`
- `make`

Instalación de dependencias:

```bash
sudo apt update
sudo apt install -y build-essential libcurl4-openssl-dev libssh2-1-dev libssl-dev
```

---

## Compilación

```bash
make
```

Para limpiar los objetos compilados:

```bash
make clean
```

---

## Ejecución

```bash
./trabajoparalelo
```

El programa solicitará:

```
Ingrese correo institucional (nombre@utem.cl): usuario@utem.cl
Ingrese RUT (12.345.678-9): 12.345.678-9
```

Las credenciales corresponden a la cuenta institucional UTEM registrada en la API.

---

## Estructura del Proyecto

```
TrabajoComputacion/
├── src/
│   ├── main.cpp        # Punto de entrada, orquestación general
│   ├── api.cpp / api.h # Cliente HTTP, autenticación JWT, cache de UUIDs
│   ├── sftp.cpp / sftp.h # Descarga de archivos desde servidor SFTP
│   ├── csv.cpp / csv.h # Parser de archivos CSV
│   ├── metrics.cpp / metrics.h # Cálculo de promedios por género
│   └── logger.cpp / logger.h   # Registro de errores en log.txt
├── downloads/          # Archivos CSV descargados (generado en ejecución)
├── uuid_cache.txt      # Cache persistente de UUID -> género
├── resultados.txt      # Resultados finales (generado en ejecución)
├── log.txt             # Registro de errores (generado en ejecución)
├── mock_api.py         # Servidor mock local para pruebas sin API real
├── Makefile
└── README.md
```

---

## Flujo del Programa

1. **Autenticación** — POST a `/v1/login/authenticate` con email y RUT, obtiene JWT.
2. **Descarga SFTP** — Conecta al servidor `137.184.45.251` y descarga todos los archivos `reporte_*.csv` en paralelo.
3. **Escaneo de UUIDs** — Recorre todos los CSV y recolecta los UUID únicos de clientes.
4. **Resolución de géneros** — Consulta la API REST `/v1/person/{uuid}` en paralelo para cada UUID no cacheado. Los resultados se guardan en `uuid_cache.txt`.
5. **Cálculo de métricas** — Asocia cada transacción al género de su cliente y calcula el promedio de `MONTO APLICADO` por género.
6. **Salida** — Imprime resultados en consola y los guarda en `resultados.txt`.

---

## Paralelismo con OpenMP

El programa utiliza OpenMP en varias etapas:

- **Descarga SFTP:** múltiples archivos se descargan concurrentemente.
- **Parseo CSV:** los archivos se procesan en paralelo con `#pragma omp parallel for`.
- **Resolución de UUIDs:** pipeline productor-consumidor con `max_concurrent` hilos consultando la API simultáneamente.
- **Cálculo de métricas:** reducción paralela de montos por género con `reduction`.

La cantidad de hilos se configura automáticamente según los núcleos disponibles (`thread::hardware_concurrency()`). Se puede sobreescribir con variables de entorno:

```bash
API_MAX_CONCURRENT=96 ./trabajoparalelo   # hilos para consultas API
API_TIMEOUT=30 ./trabajoparalelo          # timeout en segundos por request
```

---

## Archivos de Salida

### `resultados.txt`

```
FEMENINO = 15234
MASCULINO = 13220
TIEMPO = 4.52 segundos
```

### `log.txt`

Registra errores durante la ejecución, por ejemplo:

```
[ERROR] UUID no encontrado en API: 550e8400-e29b-41d4-a716-446655440000
[ERROR] Timeout al consultar UUID: ...
```

---

## Cache de UUIDs

El archivo `uuid_cache.txt` persiste los resultados de la API entre ejecuciones. Si el programa se interrumpe y se vuelve a correr, retoma desde donde quedó sin repetir consultas ya resueltas. Formato:

```
550e8400-e29b-41d4-a716-446655440000|MASCULINO
661f9511-f30c-52e5-b827-557766551111|FEMENINO
```

Para forzar una re-consulta completa, eliminar el archivo:

```bash
rm uuid_cache.txt
```

---

## Servidor Mock (Pruebas sin API real)

Si la API `api.sebastian.cl` no está disponible, se puede usar el mock local:

```bash
# Terminal 1
python3 mock_api.py

# Terminal 2 — cambiar en main.cpp:
# const string api_base = "http://localhost:3000/cpyd";
make && ./trabajoparalelo
```

El mock asigna género de forma determinística por UUID, por lo que los resultados son consistentes entre ejecuciones pero no corresponden a datos reales.

---

## Dependencias Externas

| Librería | Uso | Justificación |
|----------|-----|---------------|
| `libcurl` | HTTP (API REST) y SFTP | Librería estándar C para transferencias, soporta ambos protocolos |
| `libssh2` | Soporte SSH/SFTP bajo libcurl | Requerida por libcurl para SFTP |
| `OpenMP` | Paralelismo de hilos | Estándar para HPC en C/C++, soporte nativo en GCC |
| `OpenSSL` | TLS para HTTPS | Requerida por libcurl para conexiones seguras |