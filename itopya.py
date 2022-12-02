from bs4 import BeautifulSoup as bs
import requests
import pandas as pd
from cmath import nan
from decimal import Decimal
import pypyodbc
from ince import ince



class itopya():
    pclistitopya=[]
    for i in range(10):
        html_textitopya=requests.get("https://www.itopya.com/HazirSistemler/?sayfa="+str(i)).text
        soupitopya=bs(html_textitopya,"lxml")
        hepsiitopya=soupitopya.find("div", id="productList")
        url="https://www.itopya.com"
        pcs=hepsiitopya.find_all("div", class_="product")
        for pc in pcs:
            link=pc.find("div", class_="product-body").find("a", class_="title").get("href")
            pcname=pc.find("a", class_="title").text.replace("\n"," ")
            price=pc.find("div", class_="price")
            price=price.find("strong").text.split(">")[0].split(",")[0]
            features=pcname.split("/")
            ramm=features[3].split("GB")
            if "DDR" in features[2]:
                pcd={
                "name":features[0],
                "cpu":features[1],
                "gpu":nan,
                "ram":features[2],
                "storage":features[3],
                "price":"{:.3f}".format(float(price)),
                "link":url+link
                }
            elif int(ramm[0])>128:
                pcd={
                "name":features[0],
                "cpu":features[1],
                "gpu":features[2],
                "ram":nan,
                "storage":features[3],
                "price":"{:.3f}".format(float(price)),
                "link":url+link
                }
            else:
                pcd={
                "name":features[0],
                "cpu":features[1],
                "gpu":features[2],
                "ram":features[3],
                "storage":features[4],
                "price":"{:.3f}".format(float(price)),
                "link":url+link
                }
            pclistitopya.append(pcd)

    newgpulist=ince.gpulist
    for i in pclistitopya:
        for j in newgpulist:
            try:
                if j in i["gpu"]:
                    i["gpu"]=j
                    break
                elif "1050 TI" in i["gpu"]:
                    i["gpu"]="GTX 1050Ti"
                    break 
                elif "1660 SUPER" in i["gpu"]:
                    i["gpu"]="GTX 1660S"
            except:
                break

    pclistitopyag=[]
    for i in pclistitopya:
        try:
           g=i["gpu"].split(" ")
           i["gpu"]="".join(g)
           pclistitopyag.append(i)
        except:
           continue

    unique_gpus=[]
    for i in pclistitopya:
        if i["gpu"] not in unique_gpus:
            unique_gpus.append(i["gpu"])
    if nan in unique_gpus:
        unique_gpus.remove(nan)
    unique_gpus.sort(reverse=True)
    for i in unique_gpus:
        if i not in newgpulist:
            newgpulist.append(i)
            
    db = pypyodbc.connect(
    "Driver={SQL Server};"
    "Server=DESKTOP-ADILVNP\SQLEXPRESS;"
    "Database=test;"
    "Trusted_Connection=True;"
    )
    cursor=db.cursor()
    try:
        cursor.execute("""
        CREATE TABLE itopya (
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
        DROP TABLE itopya;
        CREATE TABLE itopya (
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
        INSERT INTO itopya
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """


    for i in pclistitopyag:
        try:
            cursor.execute(insert_statement, [i["name"],i["cpu"],i["ram"],i["gpu"],i["storage"],i["price"],i["link"]])        
        except:
            i["ram"]="null"
            cursor.execute(insert_statement, [i["name"],i["cpu"],i["ram"],i["gpu"],i["storage"],i["price"],i["link"]]) 
    print('records inserted successfully itopya')
    cursor.commit()
    db.close()
