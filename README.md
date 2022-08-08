# Big Data project of an eCommerce behavior dataset
This project is part of the evaluations within the Big Data Scala Trainee Program of Applaudo Studios
## 1. Dataset Description
### 1.1 Source and description

The Dataset used contains a total of 177,493,621 records, segmented into three files in csv format, one for each month between October and December 2019. You can consult part of the files published in [Kaggle](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) and the rest have been shared via [Google Drvie](https://drive.google.com/drive/folders/1Nan8X33H8xrXS5XhCKZmSpClFTCJsSpE) by [Michael Kechinov](https://www.linkedin.com/in/mkechinov/?originalSubdomain=ru)

Each record represents an event performed by a user regarding a product within a session, which is represented by a unique hash, each session can contain many events. Each event is like many-to-many relation between products and users.
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
The EDA results have been obtained from a 10% sample extracted using the function ``generateSample`` (see [Reader](https://github.com/hijpax/SparkPractice/blob/main/src/main/scala/Reader.scala)) which receives as parameters the path of the original dataset, the name of the file and the sampling fraction. More details about this feature are presented in the following sections.

#### Script
The Exploratory Data Analysis report was obtained with the help of the library [DataPrep](https://dataprep.ai/) supported by [Dask](https://www.dask.org/).

The script works as follows:
1. Through the ``read_parquet`` function (of the *dask.dataframe* component) the files are read in parquet format, specifying the *sourcePath* as a parameter. Get a dataframe (``df``).
2. From the obtained dataframe, the ``create_report`` function of the ``dataprep.eda`` component is used to generate the EDA report.
3. If you use a notebook such as Jupyter or Google Colab, you can directly display the result, simply by calling the ``report`` variable.
4. ``show_browser()`` allows you to open the report in the browser.
5. Finally, through the ``save`` function you can specify a path to save the result in html format.

```python
import dask.dataframe as dd
from dataprep.eda import create_report

df = dd.read_parquet("C:/data/sample/*.parquet") 
report = create_report(df)
report
report.show_browser()
report.save('C:/data/report.html')
```

#### Results
The following images show the *Overview* section of the EDA Report, in which we can highlight the following:
- The number of columns detected and analyzed is 9 and the number of records in the sample is approximately 17,754,000 ( $1,775*10^7$ ) rows.
- The missing cells are equivalent to 4.1%, which is barely acceptable for a margin of error of 5%. It should be taken into account that this percentage is based on the total number of cells, so the total number of rows with missing data could increase or decrease in the analysis of each column.
- The number of duplicate rows is 5005 (less than 1%), taking into account that each event represents a user action at a time with exact seconds, this represents possible incidents when registering in the DB; such as the network connection in client-server and server-DB communication (double sending of requests).
- We are shown the total size of the data in memory when processing it and something to take into account is the average value of row size.
- 5 categorical and 4 numerical variables are presented to take into account their formatting and treatment.

<figure>
    <img src="./img/EDA_report_overview_1.png" width="450" height="auto"/>
    <figcaption>Dataset Statistics. <i>Overview</i> section from the EDA Report with DataPrep.</figcaption>
</figure>

- The *Dataset Insights* section presents a summary of each column.
- The *category_code* column has 23.94% of missing values, so if we decided to delete each of the rows that have those null values, we would be talking about shortening the fourth part of the dataset.
- The *brand* column has a 13.21% of missing values, taking into account that both this property and the *category_code* do not represent an impediment for the rows with those null values to have meaning as if they would be, for example, *event_type* or *event_time*; you can proceed to replace them with a value like *undefined* or *not specified*.
- It is also pointed out which columns are skewed and which have a high cardinality.

<figure>
<img src="./img/EDA_report_overview_2.png" width="450" height="auto"/>
<figcaption>Dataset Insights. <i>Overview</i> section from the EDA Report with DataPrep.</figcaption>
</figure>

<figure>
<img src="./img/EDA_report_overview_3.png" width="450" height="auto"/>
<figcaption>Dataset Insights. <i>Overview</i> section from the EDA Report with DataPrep.</figcaption>
</figure>

The full report is in the folder [data-profiling](./data-profiling)
## 2. Environment

## 3. How to execute the solution?
### 3.1 Arguments
## 4. Dataset Insights and their reports 
## 5. Project structure
## 6. Challenges during development
## 7. Comments
