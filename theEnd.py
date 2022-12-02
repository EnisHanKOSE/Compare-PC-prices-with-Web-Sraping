from ince import *
from itopya import *
from sinerji import *
import datetime
ince()
itopya()
sinerji()
#date=datetime.datetime.now().replace(second=0, microsecond=0)
db = pypyodbc.connect(
    "Driver={SQL Server};"
    "Server=DESKTOP-ADILVNP\SQLEXPRESS;"
    "Database=test;"
    "Trusted_Connection=True;"
    )
cursor=db.cursor()
try:
    cursor.execute("""
    CREATE TABLE hepsi (
        PCId int NOT NULL IDENTITY PRIMARY KEY,
        name varchar(255),
        gpu varchar(255),
        price nvarchar(255),
        link varchar(255)
    );
    """)
except:
    cursor.execute("""
    DROP TABLE hepsi;
    CREATE TABLE hepsi (
        PCId int NOT NULL IDENTITY PRIMARY KEY,
        name varchar(255),
        gpu varchar(255),
        price nvarchar(255),
        link varchar(255)
    );
    """)
insert_statement = """
INSERT INTO hepsi
VALUES (?, ?, ?, ?)
"""
hepsi=ince.pclistinceg+itopya.pclistitopyag+sinerji.pclistsinerji

for i in hepsi:
    cursor.execute(insert_statement, [i["name"],i["gpu"],i["price"],i["link"]])        
print('records inserted successfully hepsi')
cursor.commit()
db.close()