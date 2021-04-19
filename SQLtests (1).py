#!/usr/bin/env python
# coding: utf-8

# In[1]:


#got this from henry
from timeit import default_timer as timer

def time_efficiency_decorator(func):
    """
    decorator to wrap function and time it
    """
    def wrapper(*args):
        start = timer()
        func(*args)
        end = timer()
        print("Starts at: {0}".format(start))
        print("Ends at: {0}".format(end))
        time_taken = end - start
        print("Time taken to execute the function: {0}".format(time_taken))
        return time_taken
    return wrapper


# In[2]:


#import libraries
import mysql.connector
from mysql.connector import Error
import pandas as pd


# In[3]:


#establish connection
#from here: https://www.freecodecamp.org/news/connect-python-with-sql/
def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database="1k_table"
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection


# In[4]:


#how do I actually establish the epassword
pw = "password"
#test connection
connection = create_server_connection("34.125.121.2", "admin", pw)


# In[5]:


#pass queries to cursor execute
#what kinda query?
#...shit, doesn't use select
#@time_efficiency_decorator
def execute_query(query):
    #change connecction to global variable, not argument
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")


# In[6]:


#this one uses select but exactly how much data do we want to select? 

#change to select row where key is rand#
@time_efficiency_decorator
def select(tableSize):
    try:
        query = ""
        if(tableSize)==1000:
            #query = "SELECT key, val from 1k_table"
            query = "SELECT key,val FROM 1k_table WHERE key = " + str(random.randint(1,table1size+1)
        if(tableSize)==100000:
            #query = "SELECT key, val from 100k_table"
            query = "SELECT key,val FROM 100k_table WHERE key = " + str(random.randint(1,table1size+1)                                                         
        #val = "SELECT col1, col2 FROM tableName"
        #call execute and pass val
        execute_query(query)
        print("Successful select query\n")
    except Error as err:
        print(f"Error: '{err}'")


# In[7]:


@time_efficiency_decorator
def insert(tableSize, value1, value2):
    #I know I'm gonna have to change the tableName variable and use our actual tableName in all caps
    #or will i?
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
        


# In[8]:


#insert(1000,1, "test")


# In[9]:


import random
import string

def randomString(chars):
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(chars))
    return result_str


# In[10]:


@time_efficiency_decorator
def insert_all(tableSize):
    key = 1
    numChar = 45 #for varchar(45) datatype
    for entry in range(tableSize):
        insert(tableSize, key, randomString(numChar))
        key+=1
        


# In[11]:


#https://www.w3schools.com/sql/sql_update.asp
@time_efficiency_decorator
def update(tableSize, numChar):
    try:
        query = ""
        #creates new random string where key = random number selected in range of tablesize
        if(tableSize==1000):
            query = "UPDATE 1k_table\nSET val = " + randomString(numchar) + "\nWHERE key = " + str(random.randint(1,table1size+1))
        if(tableSize==100000):
            query = "UPDATE 100k_table\nSET val = " + randomString(numchar) + "\nWHERE key = " + str(random.randint(1,table1size+1))
        #add execute query 
        execute_query(query)
        print("Updated successfully\n")
    except:
        print("Error updating table\n")


# In[12]:


#https://www.w3schools.com/sql/sql_delete.asp
@time_efficiency_decorator
def delete(tableSize, target):
    try:
        query = ""
        #DELETE FROM tableName WHERE col1 = target 
        if(tableSize==1000):
            query = "DELETE FROM 1k_table WHERE key = " + str(target)
        if(tableSize==100000):
            query = "DELETE FROM 100k_table WHERE key = "  + str(target)
        execute_query(query)
        print("Successfully deleted\n")
    except:
        print("Error deleting from table\n")


# In[13]:


@time_efficiency_decorator
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


# In[15]:


def performTests(tableSize, numChar):
    deleteAll(tableSize)
    insertAllTime = insert_all(tableSize)
    selectTime = select(tableSize)
    #delete a random value based on key
    keyToDelete = random.randint(1,tableSize+1) #we will reuse this value for a single insert
    deleteTime = delete(tableSize, keyToDelete)
    insertTime = insert(tableSize, keyToDelete, randomString(numChar))
    updateTime = update(tableSize, numChar)
    deleteAllTime = deleteAll(tableSize)

    return insertAllTime, selectTime, deleteTime, insertTime, updateTime, deleteAllTime
    


# In[16]:


import csv
def outputToCSV(timeResults1, timeResults2):
    with open('sql_results.csv', mode='w') as outfile:
        writer = csv.writer(outfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(['Insert All', 'Select', 'Delete', 'Insert', 'Update', 'Delete All'])
        writer.writerow(timeResults1)
        writer.writerow(timeResults2)
        


# In[17]:


table1size = 1000
table2size = 100000
numChar = 45
results1 = performTests(table1size, numChar)
results2 = performTests(table2size, numChar)
outputToCSV(results1, results2)

