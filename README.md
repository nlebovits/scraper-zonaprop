# scraper-zonaprop

Fork de Scraper de Zonaprop de Sotrosca para recuperar datos como geolocalización, inmobiliaria, entre otros. Se modificó scraper.py y con ello utils.py. De conjunto el código quedó mucho más simple pero recomiendo ver el repositorio de Sotrosca dado que es un gran ejemplo de buenas practicas y fuente de inspiración.
En un futuro agregaré data_preprocessing.py que normaliza la información volcada en el .csv y utiliza la funcion geo_data() para enriquecer el dataset con la API del Servicio de Normalización de Datos Geográficos de Argentina.

A continuación adjunto el README.md original.

## Modo de uso:

1- Instalar las dependencias declaradas en el archivo `requirements.txt`:

Con pip:

```bash
pip install -r requirements.txt
```

Con conda:

```bash
conda install --file requirements.txt
```

2- Ejecutar el script `zonaprop-scraping.py` pasando como argumento la url de la página de Zonaprop que se desea scrapear (por default se utilizará la url: https://www.zonaprop.com.ar/departamentos-alquiler.html):

```bash
python zonaprop-scraping.py <url>
```

Por ejemplo:

```bash
python zonaprop-scraping.py https://www.zonaprop.com.ar/departamentos-alquiler.html
```

3- El script generará un archivo `.csv` en el directorio `data` con los datos de los inmuebles obtenidos.

## Análisis de los datos:

Se puede ver un análisis de los datos obtenidos por el scraper en el archivo `/analysis/exploratory-analysis.ipynb`.

Tomar este análisis como un ejemplo de cómo se puede utilizar el scraper para obtener datos y analizarlos.
