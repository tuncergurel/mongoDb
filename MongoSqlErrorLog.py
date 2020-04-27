import pyodbc as tsql
from pymongo import MongoClient
import pprint

def sql():
    conn = tsql.connect("Driver={SQL Server Native Client 11.0};Server=DESKTOP-80ROL8A\SQLEXPRESS;Database=bankappdb;Trusted_Connection=yes")
    cur = conn.cursor()
    return cur

def mongo():
    client = MongoClient("localhost:27017")
    db = client.ErrorLogsDB
    return db

def main():
    cur = sql()
    db = mongo()
    #client = MongoClient("localhost:27017")
    #db = client.ErrorLogsDB

    logs = cur.execute('select * from errors')

    try:
        for i in logs:
            doc = {'Error_id': i[0], 'Error_procedure': i[1], 'Error_number': i[2],
                   'Error_Message': i[3],
                   'Error_Severity': i[4], 'line': i[5], 'datetime': i[6]}
            db.ErrorLogsDB.insert_one(doc)
        print("MongoDb'ye aktarım tamamlandı")
    except:
        print("Hata")
    try:
        cur.execute('truncate table errors')
        cur.commit()
        print('Errors tablosu boşaltıldı')
    except:
        print("Errors tablosunun silinmesi sırasında HATA oluştu")
    finally:
        cur.close()
        print("mssql server bağlantısı kesildi")

main()

