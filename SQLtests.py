#!/usr/bin/env python
# coding: utf-8

# In[22]:


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


# In[23]:


#import libraries
import mysql.connector
from mysql.connector import Error
import pandas as pd


# In[26]:


#establish connection
#from here: https://www.freecodecamp.org/news/connect-python-with-sql/
def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection


# In[27]:


#how do I actually establish the epassword
pw = "password"
#test connection
connection = create_server_connection("localhost", "admin", pw)


# In[ ]:


#we will do one table of 1k entries and one table of 100k entries
table1size = 1000
table2size = 100000
#Must test the following:
#INSERT
#UPDATE
#DELETE
#query with SELECT

#measure time using time decorator
#....how to measure consistency?


# In[ ]:


#pass queries to cursor execute
#what kinda query?
#...shit, doesn't use select
@time_efficiency_decorator
def execute_query(query):
    #change connecction to global variable, not argument
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")


# In[ ]:


#this one uses select but exactly how much data do we want to select? 
@time_efficiency_decorator
def select(tableName, value1, value2):
    try:
        val = "SELECT col1, col2 FROM tableName"
        #call execute and pass val
        execute_query(val)
        print("successful select query\n")
    except:
        print("Error with select query\n")


# In[ ]:


@time_efficiency_decorator
def insert(value1, value2):
    #I know I'm gonna have to change the tableName variable and use our actual tableName in all caps
    #or will i?
    try:
        #assume table name is table
        #do these need to be seperated by a line to work?
        query = "INSERT INTO table\n VALUES )" + str(value1) + ", " + str(value2) + ")"
        #INSERT INTO tableName
        #VALUES (value1, value2)
        #add execute query
        execute_query(query)
        print("Inserted successfully\n")
    except:
        print("Error inserting into table\n")
        


# In[6]:


#https://www.w3schools.com/sql/sql_update.asp
@time_efficiency_decorator
def update(tableName, value1, value2, col1, col2, target):
    try:
        #assume table name is table
        #UPDATE tableName
        #SET col1 = value1, col2 = value2
        #WHERE col1 = target
        query = "UPDATE table\nSET " + str(col1) + "=" + str(value1) + ", " + str(col2) + "=" + str(value2) "\nWHERE " + str(col1) + "=" + str(target)
        #add execute query 
        execute_query(query)
        print("Updated successfully\n")
    except:
        print("Error updating table\n")


# In[7]:


#https://www.w3schools.com/sql/sql_delete.asp
@time_efficiency_decorator
def delete(tableName, col1, target):
    try:
        #DELETE FROM tableName WHERE col1 = target 
        query = "DELETE FROM table WHERE " + str(col1) + "=" + str(target)
        execute_query(query)
        print("Successfully deleted\n")
    except:
        print("Error deleting from table\n")


# In[ ]:









