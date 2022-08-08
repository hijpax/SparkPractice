# Big Data project of an eCommerce behavior dataset
Este proyecto forma parte de las evaluaciones dentro del  Programa de Entrenamiento para Big Data Enginier de Applaudo Studios
## 1. Dataset Description
### 1.1 Source and description
El Dataset utilizado contiene un total de 177,493,621 de registros, segmentados en tres archivos en formato csv, uno para cada mes entre Octubre y Diciembre del 2019. Puede consultar parte de los archivos publicados en [Kaggle](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) y el resto han sido compartidos mediante [Google Drvie](https://drive.google.com/drive/folders/1Nan8X33H8xrXS5XhCKZmSpClFTCJsSpE) por [Michael Kechinov](https://www.linkedin.com/in/mkechinov/?originalSubdomain=ru)


Cada registro representa un evento realizado por un usuario con respecto a un producto dentro de una sesion, la cual se representa por un hash unico, cada sesión puede contener muchos eventos. Each event is like many-to-many relation between products and users.
#### File Structure
|    Column     | Description                                                                                                                                                      |
|:-------------:|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|  event_time   | Time when event happened at (in UTC).                                                                                                                            |
|  event_type   | Only one kind of event per register                                                                                                                              |
|  product_id   | ID of a product                                                                                                                                                  |
|  category_id  | Product's category ID                                                                                                                                            |
| category_code | Product's category taxonomy (code name) if it was possible to make it. Usually present for meaningful categories and skipped for different kinds of accessories. |
|     brand     | Downcast string of brand name. Can be missed.                                                                                                                    |
|     price     | Float price of a product. Present.                                                                                                                               |
|    user_id    | Permanent user ID.                                                                                                                                               |
| user_session  | Temporary user's session ID. Same for each user's session. Is changed every time user come back to online store from a long pause.                               |

#### Event types
Events can be:

- view - a user viewed a product
- cart - a user added a product to shopping cart
- remove_from_cart - a user removed a product from shopping cart
- purchase - a user purchased a product

#### How to read it
Semantics (or how to read it):
> User (**user_id**) during session (**user_session**) added to shopping cart (property **event_type** is equal *cart*) product (**product_id**) of **brand** of category (**category_code**) with **price** at **event_time**.

### 1.2 EDA report
#### Obtencion de la muestra
Los resultados del EDA se han obtenido de una muestra del 10% extraida mediante la funcion ``generateSample`` (ver [Reader](https://github.com/hijpax/SparkPractice/blob/main/src/main/scala/Reader.scala)) la cual recibe como parametros el path del dataset original, el nombre del archivo y la fraccion de muestreo. En las secciones siguientes se presentan más detalles sobre esta funcion.

#### Script
El reporte de Exploratory Data Analysis se obtuvo con ayuda de la libreria [DataPrep](https://dataprep.ai/) apoyada en [Dask](https://www.dask.org/).

El script trabaja de la siguiente forma:
1. Por medio función ``read_parquet`` (del componente dask.dataframe) se leen los archivos en formato parquet, especificando el *sourcePath* como parametro. Se obtiene un dataframe (``df``).
2. A partir del dataframe obtenido, se utiliza la funcion ``create_report`` del componente ``dataprep.eda`` para generar el reporte EDA.
3. Si se utiliza un notebook como Jupyter o Google Colab, se pude mostrar directamente el resultado, simplemente con invocar la variable ``report``
4. ``show_browser()`` permite abrir el reporte en el navegador.
5. Finalmente, por medio de la funcion ``save`` se puede especificar una ruta para guardar el resultado con formato html.

```python
import dask.dataframe as dd
from dataprep.eda import create_report

df = dd.read_parquet("C:/data/sample/*.parquet") 
report = create_report(df)
report
report.show_browser()
report.save('C:/data/report.html')
```

### Results
En la siguientes imagenes se observa la seccion de *Overview* del Reporte EDA, en la que podemos destacar lo siguiente:
- La cantidad de columnas detectadas y analizadas es 9 y la cantidad de registros de la muestra es de aproximadamente 17,754,000 ( $1.775*10^7$ ) rows. 
- Las missing cells equivalen a un 4.1%, es apeas eceptable para un margen de error del 5%. Se debe tomar en cuenta que este porcentaje es apartir del total de celdas, por lo que el total de rows con missing data podria aumentar o disminuir en el analisis de cada columna. 
- La cantidad de rows duplicadas es de 5005 (menos del 1%), tomando en cuenta que cada evento representa una accion del usuario en un momento con exactitud de segundos, esto represena posibles incidentes al momento de regitrar en la BD; como por ejemplo la conexion de red en la comunicacion cliente-servidor y servidor-DB (doble envio de peticiones).
- Se nos muestra el total size de la data en memoria al procesarla y algo a tomar en cuenta es el valor promedio de row size.
- Se presentan 5 variables categoricas y 4 numericas para tomar en cuenta su formateo y tratamiento.

<figure>
    <img src="./img/EDA_report_overview_1.png" width="450" height="auto"/>
    <figcaption>Dataset Statistics. Seccion de *Overview* from the EDA Report with DataPrep.</figcaption>
</figure>

- The *Dataset Insights* sections presents a summary of each column.
- The *category_code* column tiene un 23.94% de missing values, por lo que si se decidiera borrar cada una de las rows que presentan esos valores nullos, estariamos hablando de acortar la cuarta parte del dataset.
- The *brand* colunm posee un 13.21% de missing values, tomando en cuenta que tanto esta propiedad como el *category_code* no representan impedimento para que las rows con esos valores nulos tengan significado como si lo serian, por ejemplo, *event_type* o *event_time*; se puede proceder a reemplazarlos con un valor como *undefined* o *not specified*.
- Se senalan tambien que columnas son skewed y cuales poseen una alta cardinalidad.

<figure>
<img src="./img/EDA_report_overview_2.png" width="450" height="auto"/>
<figcaption>Dataset Insights. Seccion de *Overview* from the EDA Report with DataPrep.</figcaption>
</figure>

<figure>
<img src="./img/EDA_report_overview_3.png" width="450" height="auto"/>
<figcaption>Dataset Insights. Seccion de *Overview* from the EDA Report with DataPrep.</figcaption>
</figure>

El reporte completo se encuentra en la carpeta [data-profiling](./data-profiling)
## 2. Environment

## 3. How to execute the solution?
### 3.1 Arguments
## 4. Dataset Insights and their reports 
## 5. Project structure
## 6. Challenges during development
## 7. Comments
