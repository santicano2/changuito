# Changuito

Aplicacion web para seguir precios de productos en supermercados de Argentina usando datos abiertos de SEPA (Precios Claros).

## Objetivo

- Funcionar sin backend propio
- Usar solo herramientas gratuitas
- Mostrar evolucion de precios por producto

## Tecnologias

- Frontend estatico: `index.html`, `styles.css`, `app.js`
- Ingesta de datos: `scripts/update_data.py`
- Automatizacion diaria: `.github/workflows/update-data.yml`
- Publicacion recomendada: GitHub Pages

## Estructura del proyecto

- `index.html`: interfaz principal
- `styles.css`: estilos
- `app.js`: logica de busqueda, ubicacion y grafico
- `scripts/update_data.py`: descarga y proceso de datos SEPA
- `data/products.json`: productos procesados
- `data/stores.json`: sucursales procesadas
- `data/histories.json`: historial de precios
- `data/meta.json`: metadatos de la ultima actualizacion
- `data/snapshots/`: resumen por corrida

## Requisitos

- Python 3.10 o superior

## Uso local

1) Generar datos:

```bash
python scripts/update_data.py
```

2) Levantar servidor local:

```bash
python -m http.server 8000
```

3) Abrir en el navegador:

`http://localhost:8000`

## Variables opcionales

- `PROVINCES`: provincias a incluir separadas por coma
- `MAX_RECORDS_PER_DAY`: maximo de registros por dia
- `MAX_SNAPSHOTS`: cantidad de snapshots a conservar
- `MAX_RESOURCES_PER_RUN`: cantidad de dias a procesar por corrida
- `MAX_PRODUCTS`: limite de productos nuevos por corrida
- `MAX_STORES`: limite de sucursales nuevas por corrida
- `RESET_DATA=true`: reconstruye archivos de `data/` desde cero

Ejemplo:

```bash
PROVINCES="AR-C,AR-B" MAX_RESOURCES_PER_RUN=5 MAX_PRODUCTS=5000 python scripts/update_data.py
```

## Fuente de datos

- CKAN: `https://datos.produccion.gob.ar/api/3/action/package_show?id=sepa-precios`
- Licencia: `CC-BY-4.0`

## Notas

- El dataset original es grande. Para mantener costo cero, el script aplica filtros y limites.
- La app funciona con archivos JSON estaticos generados por el pipeline.
