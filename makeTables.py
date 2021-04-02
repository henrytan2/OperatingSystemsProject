#!/usr/bin/env python
# coding: utf-8

# In[1]:


#used to make our tables ahead of time
#import libraries
import mysql.connector
from mysql.connector import Error
import sys
import random
import string


#it works!
#get command line arguments minus the program name
args = sys.argv[1:]

#okay, we're gonna run it like Python3 makeTables.py <ip address for VM connecton> <database/schema name> <username> <password>

#establish connection
#from here: https://www.freecodecamp.org/news/connect-python-with-sql/
def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database= args[1]
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection


#get connection as global variable
#pw = "password"
#test connection
#connection = create_server_connection("34.125.121.2", "admin", pw) --> this is what julie actually uses for GCP
connection = create_server_connection(args[0], args[2], args[3])
#okay, we're gonna run it like Python3 makeTables.py <ip address for VM connecton> <database name> <username> <password>
#suggested --> whatever is in your VM / 1k_table / admin / password


# In[ ]:


#i'm making a decision that we can all name our tables 1k_table and 100k_table but if we need to pass it as anoter command line argument, i can do that
#pass queries to cursor execute
def execute_query(query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")
        
def randomString(chars):
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(chars))
    return result_str
        
#insert function for use with insert all       
def insert(tableSize, value1, value2):
    try:
        query = ""
        if(tableSize==1000):
            query = "INSERT INTO 1k_table\n VALUES (" + str(value1) + ", '" + value2 + "')"
        if(tableSize==100000):
            query = "INSERT INTO 100k_table\n VALUES (" + str(value1) + ", '" + value2 + "')"
        #add execute query
        execute_query(query)
        print("Inserted successfully\n")
    except Error as err:
        print(f"Error: '{err}'")
        
        
#function to insert all values into the table
def insertAll(tableSize):
    key = 1
    numChar = 45 #for varchar(45) datatype
    for entry in range(tableSize):
        insert(tableSize, key, randomString(numChar))
        key+=1
        
        
#function to clear all values from the table
def deleteAll(tableSize):
    try:
        query = ""
        if(tableSize==1000):
            query = "DELETE FROM 1k_table"
        if(tableSize==100000):
            query = "DELETE FROM 100k_table"
        execute_query(query)
        print("Cleared table successfully\n")
    except Error as err:
        print(f"Error: '{err}'")
        
def clearAndFill(tableSize):
    deleteAll(tableSize)
    insertAll(tableSize)


# In[ ]:



#get the tables instantiated ahead of time
table1size = 1000
table2size = 100000


clearAndFill(table1size)
clearAndFill(table2size)

