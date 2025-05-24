# scraper-zonaprop

Fork del repositorio original de Sotrosca. Este scraper recupera datos de propiedades de ZonaProp, incluyendo información detallada como ubicación, características, precios, y datos de la inmobiliaria.

## Requisitos importantes

⚠️ **IMPORTANTE**: Este scraper requiere una dirección IP de Argentina para funcionar correctamente. ZonaProp bloquea el acceso desde IPs de otros países.

Si estás fuera de Argentina, necesitarás usar una VPN configurada con una ubicación en Argentina. Algunas recomendaciones:
- Asegúrate de que tu VPN esté correctamente configurada y conectada antes de ejecutar el scraper
- Si el scraper falla con un error de "NoneType", verifica que tu VPN esté funcionando correctamente
- Algunas VPNs pueden no funcionar correctamente con ZonaProp; si tienes problemas, prueba con un proveedor de VPN diferente

## Modo de uso:

1- Clonar el repositorio:

```bash
git clone https://github.com/yourusername/scraper-zonaprop.git
cd scraper-zonaprop
```

2- Instalar las dependencias usando `uv`:

```bash
uv sync
```

Esto creará un entorno virtual e instalará todas las dependencias necesarias.

3- Ejecutar el script principal:

```bash
uv run zonaprop-scraping.py
```

Por defecto, el script utilizará la URL: https://www.zonaprop.com.ar/departamentos-alquiler.html. Para cambiar la URL, edita el archivo `zonaprop-scraping.py` directamente.

### Flags disponibles:

El script acepta los siguientes flags:

- `--url`: URL directa de ZonaProp a scrapear
- `--property-types` o `-p`: Tipos de propiedades a scrapear (pueden especificarse múltiples)
  - Opciones válidas: departamentos, casas, terrenos, locales-comerciales, ph
- `--transaction-type` o `-t`: Tipo de transacción (venta o alquiler)
- `--limit` o `-l`: Límite en el número de resultados a scrapear

Ejemplos de uso:

```bash
# Scrapear una URL específica
uv run zonaprop-scraping.py --url https://www.zonaprop.com.ar/departamentos-venta.html

# Scrapear múltiples tipos de propiedades
uv run zonaprop-scraping.py -p departamentos casas -t venta

# Limitar el número de resultados
uv run zonaprop-scraping.py -p departamentos -l 100
```

3- El script generará un archivo `.csv` en el directorio `data` con los datos de los inmuebles obtenidos.

## Análisis de los datos:

Se puede ver un análisis de los datos obtenidos por el scraper en el archivo `/analysis/exploratory-analysis.ipynb`.

Tomar este análisis como un ejemplo de cómo se puede utilizar el scraper para obtener datos y analizarlos.

## Mejoras futuras

- Implementar `data_preprocessing.py` para normalizar la información y enriquecer el dataset con la API del Servicio de Normalización de Datos Geográficos de Argentina
- Optimizar las requests para mejorar la velocidad de scraping
- Implementar caché de datos en formato Parquet para consultas más rápidas
- Agregar limpieza general de datos y validación de campos