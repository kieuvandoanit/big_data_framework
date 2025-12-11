import os
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    return mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password="root_result_password",
        database="retail_analytics",
        port=3307
    )