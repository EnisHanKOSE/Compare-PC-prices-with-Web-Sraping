from bs4 import BeautifulSoup as bs
import requests
import pandas as pd
import datetime
from cmath import nan
import pypyodbc
    
class ince():
    gpulist=["RX 6800","RX 6750 XT","RX 6700 XT","RX 6650 XT","RX 6600","RX 6500 XT","RX 560","RX 5500 XT","RTX 4080","RTX 3060","RTX 3050","RTX 2060","GTX 1660S","GTX 1660 Ti",
    "GTX 1650","GTX 1630","GTX 1050 Ti","GT 730","GT 1030","RTX 3080","RTX 3080 Ti","RTX 3070","RTX 3070 Ti","RTX 3090","RTX 3090 Ti"]
    pclistince=[]      
    for i in range(10):
        html_textince=requests.get("https://www.incehesap.com/hazir-sistemler-fiyatlari/sayfa-"+str(i)+"/").text
        soupince= bs(html_textince, "lxml")
        hepsi=soupince.find("div", class_="grid grid-cols-2 md:grid-cols-3 gap-1")
        url="https://www.incehesap.com"
        try:
            pcs=hepsi.find_all("a", href=True)
            for pc in pcs:
                link=pc.get("href")
                price=pc.find("span", class_="mx-auto whitespace-nowrap text-lg font-bold leading-none tracking-tight text-orange-500 md:text-2xl mb-2")
                pcname=pc.find("div", class_="line-clamp-2 h-11 text-center leading-tight px-1 lg:px-4 md:space-x-3").text.replace("\n"," ")
                features=pcname.split("|")
                if price!=None:
                    price=pc.find("span", class_="mx-auto whitespace-nowrap text-lg font-bold leading-none tracking-tight text-orange-500 md:text-2xl mb-2").text.replace(" ","").replace("\n","")
                    try:
                        storage=features[4].split()
                        price=price.split(",")
                        gpu=features[3].strip().split(" ")[:-2]
                        gpu=" ".join(gpu)
                        if "Radeon" in gpu:
                            gpu=gpu.split(" ")[1:]
                            gpu=" ".join(gpu)
                            pcd={
                            "name":features[0],
                            "cpu":features[1],
                            "ram":features[2],
                            "gpu":gpu,
                            "storage":storage[0]+" "+ storage[1]+" "+storage[2],
                            "price":"{:.3f}".format(float(price[0])),
                            "link":url+link
                            }
                            pclistince.append(pcd)
                        else:
                            pcd={
                            "name":features[0],
                            "cpu":features[1],
                            "ram":features[2],
                            "gpu":gpu,
                            "storage":storage[0]+" "+ storage[1]+" "+storage[2],
                            "price":"{:.3f}".format(float(price[0])),
                            "link":url+link
                            }
                            pclistince.append(pcd)
                    except:
                        storage=features[3].split()
                        price=price.split(",")
                        pcd={
                        "name":features[0],
                        "cpu":features[1],
                        "ram":features[2],
                        "gpu":nan,
                        "storage":storage[0]+" "+ storage[1]+" "+storage[2],
                        "price":"{:.3f}".format(float(price[0])),
                        "link":url+link
                        }
                        pclistince.append(pcd)
        except:
            break
          
    unique_gpus=[]
    for i in pclistince:
        if i["gpu"] not in unique_gpus:
            unique_gpus.append(i["gpu"])
    if nan in unique_gpus:
        unique_gpus.remove(nan)
    for i in unique_gpus:
        if i not in gpulist:
            gpulist.append(i)
    if nan in gpulist:
        gpulist.remove(nan)
    gpulist.sort(reverse=True)
    pclistinceg=[]
    for i in pclistince:
        try:
            g=i["gpu"].split(" ")
            i["gpu"]="".join(g)
            pclistinceg.append(i)
        except:
            continue
    gpulistg=[]
    for i in pclistinceg:
        if i["gpu"] not in gpulistg:
            gpulistg.append(i["gpu"])
    gpulistg.sort(reverse=True)
    db = pypyodbc.connect(
    "Driver={SQL Server};"
    "Server=DESKTOP-ADILVNP\SQLEXPRESS;"
    "Database=test;"
    "Trusted_Connection=True;"
    )
    cursor=db.cursor()
    try:
        cursor.execute("""
        CREATE TABLE ince (
            PCId int NOT NULL IDENTITY PRIMARY KEY,
            name varchar(255),
            cpu varchar(255),
            ram varchar(255),
            gpu varchar(255),
            storage varchar(255),
            price nvarchar(255),
            link varchar(255)
        );
        """)
    except:
        cursor.execute("""
        DROP TABLE ince;
        CREATE TABLE ince (
            PCId int NOT NULL IDENTITY PRIMARY KEY,
            name varchar(255),
            cpu varchar(255),
            ram varchar(255),
            gpu varchar(255),
            storage varchar(255),
            price nvarchar(255),
            link varchar(255)
        );
        """)
    insert_statement = """
    INSERT INTO ince
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """

    for i in pclistinceg:
        cursor.execute(insert_statement, [i["name"],i["cpu"],i["ram"],i["gpu"],i["storage"],i["price"],i["link"]])        

    print('records inserted successfully ince')
    cursor.commit()
    db.close()
