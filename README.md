# Large File Processing
Import CSV in a SQL database

## Steps to run your code.
#### Requirements
1) Clone the repo and get into to directory _Large-File-Processor_
2)  Make sure python3 and MySQL are installed on the system and configure the following in the code. If user and password is different then make sure to change it in 
```
host="localhost"
user="root"
password="12345"
```

3) Use the package manager pip to install [MYSQL-python](https://pypi.org/project/MySQL-python/).
```bash
pip install MySQL-python
```
MYSQL-python is a connector that is used to make SQL queries in python.

4) Download the [products.csv.gz](https://drive.google.com/drive/folders/1X3qomdbjWU1oOTbBvxchTzjLMAwYBWFT) file if not present and store it in the same directory where code is present

#### Steps:
1) Make sure SQL server is running
2) Use the code to run the program:
```
python main.py
```
Uncomment the line `db.drop_table()` to drop the table
Uncomment the line `db.createa_aggregate_table()` to create the aggregate table
Uncomment the line `db.upload_to_db()` to upload the data file to the database

In case of a sku value match it will simply update the existing value



## Details of all the tables and their schema
2 tables are created. products table is created for data ingestion from the csv file. aggregate table is created to contains name, no. of products

```mysql
CREATE TABLE products (
    name VARCHAR(255) NOT NULL, sku VARCHAR(255) NOT NULL PRIMARY KEY, 
    description VARCHAR(1024) NOT NULL
    );
    
CREATE TABLE aggregate AS SELECT name, count(*) as no_of_products
    FROM products GROUP BY name;
```


## Done from points to achieve:
1) Your code should follow concept of OOPS
2) Support for updating existing products in the table based on `sku` as the primary key. (Yes, we know about the kind of data in the file. You need to find a workaround for it)
3) All product details are to be ingested into a single table
4) An aggregated table on above rows with `name` and `no. of products` as the columns

**Products Table**

| name  | sku | Description |
| ------------- | ------------- | ------------------- |
| 'James Oconnor'  | a-ability-see-gun  | According member fine program. Concern single too ahead my. Loss onto which include listen later present. Election sport quite notice why. Find system writer might. Sing prevent compare black. |
| 'Jennifer Lambert'  | a-above-its-focus  | Go audience old. Law main federal area myself. Leave various leave discover consumer hotel. Safe fall up compare plant affect stuff. |
| 'Christopher Tate' | a-act-cut-either | Data tell enter. Because stock along continue follow respond off value. Trial try exactly type simply full. |
| 'Vicki Barber' | a-act-spring-camera | Difference compare society best structure democratic team machine. Administration item light among. Each when capital condition election miss defense. Left after treat listen law girl. |
| 'Tina Dunn' | a-activity | Discussion itself those stand beat Mr. Any from event. Training cultural avoid artist quite say figure. Play fill cultural know education arm rate reflect. Me investment pull star. |

**Aggregate Table**

| name  | no_of_products |
| ------------- | ------------- |
| 'James Oconnor'  | 5  |
| 'Jennifer Lambert'  | 7  |
| 'Christopher Tate' | 5 |
| 'Vicki Barber' | 1 |
| 'Tina Dunn' | 3 |

## Points left to achieve:
1) Support for regular non-blocking parallel ingestion of the given file into a table. Consider thinking about the scale of what should happen if the file is to be processed in 2 mins.
**Note:** _Since the parallel ingestion is not applied it may take a bit time to process._
2) Haven't put in docker container yet

## Improvement to be done if provided more days
1) I'll be parallelize the task to run as effectiently as possible
2) I'll refactor the code
3) I'll put in a docker container to be more accessible