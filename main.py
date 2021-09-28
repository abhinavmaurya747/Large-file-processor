import gzip
import pandas as pd
import mysql.connector
from multiprocessing import Pool
#from pathos.multiprocessing import ProcessingPool as Pool
import time

gzfile = 'products.csv.gz'

def loadcsv(gzfile):
    '''
        load the csv file from gz file
        gzfile : name of the gz file which contains the csv file
        returns (
                data - the data read from the csv file in a pandas DataFrame format
                )
    '''
    with gzip.open(gzfile) as f:
        data = pd.read_csv(f)
    return data

    # Check for the shape of the data. It must be (500000, 3)
    # print(data.shape)
    # print(data.describe())

    # Data contains 3 columns: name, sku, description

class databaseAPI:
    '''
        The database API class with functionalities to connect to sql, connect to database, create database, 
        create table, check table, check database, drop table, upload some data to database, list information about a table

        class variables (
            DB_EXISTS - contains True/False on the basis of existence of the databases csvdb
            TB_EXISTS - contains True/False on the basis of existence of the tables
            mydb      - a connection to the database
            mycursor  - a cursor to the connection for executing an SQL statement
            )
    '''
    # check variables
    DB_EXISTS = False
    TB_EXISTS = False
    mydb,mycursor =None, None

    def __init__(self):
        pass

    def connect_to_sql(self,  u="root", pwd="12345"):
        # Connect to sql and check if our database exists
        self.mydb = mysql.connector.connect(
            host="localhost",
            user=u,
            password=pwd
        )
        self.mycursor = self.mydb.cursor()

    def check_db(self):
        # Check if the database exists or not and update the DBEXISTS variable
        self.mycursor.execute("SHOW DATABASES")
        for x in self.mycursor:
            if x[0] == 'csvdb':
                self.DB_EXISTS = True
                print("Database already exists!")
        

    def create_db(self):
        self.check_db()
        # If the database 'csvdb' doesn't exists then create it
        if self.DB_EXISTS == False:
            self.mycursor.execute("CREATE DATABASE csvdb")
            print("Database successfully created!")


    def connect_db(self, u="root", pwd="12345", db="csvdb"):
        # Connect to our MYSQL database
        self.mydb = mysql.connector.connect(
            host="localhost",
            user=u,
            password=pwd,
            database=db
        )
        self.mycursor = self.mydb.cursor()

    def check_table(self):
        # Check if products table exists
        self.mycursor.execute("SHOW TABLES")
        for x in self.mycursor:
            if x[0] == "products":
                self.TB_EXISTS = True
                print("products table already exists!")
           

    def drop_table(self):
        # To drop a table
        self.mycursor.execute("DROP TABLE products")
        self.TB_EXISTS = False
        print("Table destroyed!")


    def create_table(self):
        self.check_table()
        # Create the table if it doesn't exists
        if  self.TB_EXISTS == False:
            self.mycursor.execute(
                "CREATE TABLE products (name VARCHAR(255) NOT NULL, sku VARCHAR(255) NOT NULL PRIMARY KEY, description VARCHAR(1024) NOT NULL)")
            print("products table successfully created!")


    def upload_to_db(self , data):
        # Add data from csv file to database
        sql_ins_prod = "REPLACE INTO products VALUES(%s, %s, %s)"
        val = list(data.to_numpy())
        val = tuple(map(tuple, val))

        t0 = time.time()
        print("GOING TO UPLOAD!")
        
        '''
        #Passing 1 value at a time
        for i in range(len(val)):
            print(val[i])
            if (i+1)%10000 == 0 and i != 0:
                print(i," values inserted!")
            self.mycursor.execute(sql_ins_prod, val[i])
        '''
        PROCESS_LIM = 20000
        for i in range(len(val)//PROCESS_LIM):
            self.mycursor.executemany(sql_ins_prod, val[i*PROCESS_LIM:(i+1)*PROCESS_LIM])   
            print("Inserted {} values".format((i+1)*PROCESS_LIM))

        self.mycursor.executemany(sql_ins_prod, val[(len(val)//PROCESS_LIM)*PROCESS_LIM:])
        
        self.mydb.commit()
        t1 = time.time()
        print("uploading all the data to db took,{0} s".format(t1-t0))
        print("all values with unique sku column inserted!")

    def list_info(self):
        self.mycursor.execute("SELECT COUNT(name) FROM products")
        print("Number of rows in products table: {}".format([i[0] for i in self.mycursor][0]))
        self.mycursor.execute("SELECT COUNT(name) FROM aggregate")
        print("Number of rows in aggregate table: {}".format([i[0] for i in self.mycursor][0]))
        self.mycursor.execute("SELECT * FROM products LIMIT 5")
        with open("product_first_5.txt", "a") as f:
            for i in self.mycursor:
                f.writelines(str(i))
        self.mycursor.execute("SELECT * FROM aggregate LIMIT 5")
        with open("aggregate_first_5.txt", "a") as f:
            for i in self.mycursor:
                f.writelines(str(i))
        

        '''
        c=0
        print("First 5 values of db")
        for val in self.mycursor:
            if(c<=5):
                print(val)    
            c+=1
        print("Total Rows in db ", c)
        '''
    def create_aggregate_table(self):
        exists = False
        self.mycursor.execute("SHOW TABLES")
        for x in self.mycursor:
            if x[0]=='aggregate':
                print("Aggregate Table already exists!")
                exists = True
        if exists == False:
            self.mycursor.execute("CREATE TABLE aggregate AS SELECT name, count(*) as no_of_products FROM products GROUP BY name")
            

            print("Values inserted into aggregate table!")



if __name__ == '__main__':
    global db
    db = databaseAPI()

    username = "root"
    password = "12345"

    data = loadcsv(gzfile)
    
    db.connect_to_sql(u="root", pwd="12345")
    db.create_db()
    db.connect_db(u="root", pwd="12345")
    db.create_table()
    #db.drop_table()
    t=time.time()
    #db.upload_to_db(data)
    print(time.time()-t)
    db.create_aggregate_table()

    '''
    pool = Pool()
    pool.apply_async(upload_to_db, args=(db, data))
    pool.close()
    pool.join()
    '''
    '''
    with Pool(10) as p:
        
        #p.map(db.upload_to_db, data)
        #p.map(db.square, [1,2,3])
        #p.apipe(db.square, db, 10)
        p.imap(upload_to_db, data)
        #p.map(upload_to_db, args=(db, data))
    '''
    
    db.list_info()


