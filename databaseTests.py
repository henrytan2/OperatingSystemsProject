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


# In[5]:


#used to make our tables ahead of time
#import library
import sys

#it works!
#get command line arguments minus the program name
args = sys.argv[1:]

#okay, we're gonna run it like Python3 databaseTests.py <ip address for VM connecton> <schema name> <username> <password>

#establish connection
#from here: https://www.freecodecamp.org/news/connect-python-with-sql/
def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database= args[1],
            buffered=True
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


# In[6]:


#pass queries to cursor execute
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


# In[20]:


#select row where key is random number
@time_efficiency_decorator
def select(tableSize):
    try:
        query = ""
        if tableSize==1000:
<<<<<<< HEAD
            #query = "SELECT key, val from 1k_table"
            query = "SELECT `key`,`val` FROM 1k_table WHERE val = " + "'pmgyywpocfecutunvxpdeviimbftumqtbgxwytgcjlnof'" 
        if tableSize==100000:
            #query = "SELECT key, val from 100k_table"
            query = "SELECT `key`,`val` FROM 100k_table WHERE val = " + "'vhtsqarnglrjqjxtqdkzmmhdnztqycdhrpehgympahysl'" 
=======
            #query = "SELECT key, value from 1k_table"
            query = "SELECT * FROM 1k_table WHERE value = " + "'jnualvxwjhjbghnucucddzmdrfqfkbkgzqswvfvdbhgvo'"
        if tableSize==100000:
            #query = "SELECT key, value from 100k_table"
            query = "SELECT * FROM 100k_table WHERE value = " + "'cewvjuidssbkxnbmonpfgndirgjwwilrfqmihamggskcm'"
>>>>>>> 6c72c5fd8c21742ca9c1f60fb9fa96ba4db856aa
        #val = "SELECT col1, col2 FROM tableName"
        #call execute and pass val
        execute_query(query)
    except Error as err:
        print(f"Error: '{err}'")


# In[ ]:


@time_efficiency_decorator
def insert(tableSize, value1, value2):
    #I know I'm gonna have to change the tableName variable and use our actual tableName in all caps
    #or will i?
    try:
        query = ""
        if tableSize==1000:
            query = "INSERT INTO 1k_table\n VALUES (" + str(value1) + ", '" + value2 + "')" 
        if tableSize==100000:
            query = "INSERT INTO 100k_table\n VALUES (" + str(value1) + ", '" + value2 + "')" 
        #add execute query
        execute_query(query)
    except Error as err:
        print(f"Error: '{err}'")
        


# In[23]:


import random
import string

def randomString(chars):
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(chars))
    return result_str


# In[24]:


#make table ahead of time and don't measure insert_all or delete_all
#@time_efficiency_decorator
def insert_all(tableSize):
    key = 1
    numChar = 45 #for varchar(45) datatype
    for entry in range(tableSize):
        insert(tableSize, key, randomString(numChar))
        key+=1
        


# In[35]:


#https://www.w3schools.com/sql/sql_update.asp
@time_efficiency_decorator
def update(tableSize, numchar):
    try:
        query = ""
        #creates new random string where key = random number selected in range of tablesize
        if tableSize==1000:
            query = "UPDATE 1k_table\nSET `value`='" + randomString(numchar) + "'\nWHERE `key` = " + str(random.randint(1,tableSize+1)) + ";"
        if tableSize==100000:
            query = "UPDATE 100k_table\nSET `value`='" + randomString(numchar) +"'\nWHERE `key` = " + str(random.randint(1,tableSize+1)) + ";"
        #add execute query 
        execute_query(query)
    except Error as err:
        print(f"Error: '{err}'")
        
#query = "UPDATE 1k_table\nSET val = " + randomString(45) + "\nWHERE `key` = " + str(random.randint(1,table1size+1)) + ";"
#execute_query(query)


# In[ ]:


#https://www.w3schools.com/sql/sql_delete.asp
@time_efficiency_decorator
def delete(tableSize, target):
    try:
        query = ""
        #DELETE FROM tableName WHERE col1 = target 
        if tableSize==1000:
            query = "DELETE FROM 1k_table WHERE `key` = '" + str(target) + "'"
        if tableSize==100000:
            query = "DELETE FROM 100k_table WHERE `key` = '"  + str(target) + "'"
        execute_query(query)
    except Error as err:
        print(f"Error: '{err}'")


# In[ ]:


#@time_efficiency_decorator
def deleteAll(tableSize):
    try:
        query = ""
        if tableSize==1000:
            query = "DELETE FROM 1k_table"
        if tableSize==100000:
            query = "DELETE FROM 100k_table"
        execute_query(query)
        print("Cleared table successfully\n")
    except Error as err:
        print(f"Error: '{err}'") 


# In[ ]:


#this function assumes we already have our tables built
import numpy as np
def performTests(tableSize, numChar):
    
    #Follwing two lines are for testing purposes; we will create the table prior to running tests
    #deleteAll(tableSize)
    #insert_all(tableSize)
    
    #declare list of 100 elements for each benchmark measured; will delcare as empty
    #need to perform inserts and deletes at the same time to keep the list size the same
    selectTime = [0] * 100
    deleteTime = [0] * 100
    insertTime = [0] * 100
    updateTime = [0] * 100
    
    
    #perform select, update, delete, and insert
    for i in range(100):
        updateTime[i] = update(tableSize, numChar)
        #need to perform delete and insert together to keep list size the same
        keyToDelete = random.randint(1,tableSize+1) #we will reuse this value for a single insert
        deleteTime[i] = delete(tableSize, keyToDelete)
        insertTime[i] = insert(tableSize, keyToDelete, randomString(numChar))
        selectTime[i] = select(tableSize)
    
    #get averages
    avgSelect = np.average(selectTime)
    avgDelete = np.average(deleteTime)
    avgInsert = np.average(insertTime)
    avgUpdate = np.average(updateTime)
    
    #returns tuple of averages
    return avgSelect, avgDelete, avgInsert, avgUpdate, selectTime, deleteTime, insertTime, updateTime
    


# In[ ]:


import csv
def outputToCSV(timeResults1, timeResults2):
    with open('sql_results.csv', mode='w') as outfile:
        writer = csv.writer(outfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(['TRAIL NUM','1k SELECT', '1k DELETE', '1k INSERT', '1k UPDATE', '100K SELECT', '100K DELETE', '100K INSERT', '100K UPDATE'])
        for i in range(100):
            writer.writerow([i, timeResults1[4][i], timeResults1[5][i], timeResults1[6][i], timeResults1[7][i], timeResults2[4][i], timeResults2[5][i], timeResults2[6][i], timeResults2[7][i]])
        
        writer.writerow(['AVERAGES'])
        writer.writerow(['', timeResults1[0],timeResults1[1],timeResults1[2],timeResults1[3], timeResults2[0],timeResults2[1],timeResults2[2],timeResults2[3]])
        


# In[8]:


table1size = 1000
table2size = 100000
numChar = 45
results1 = performTests(table1size, numChar)
results2 = performTests(table2size, numChar)
outputToCSV(results1, results2)

