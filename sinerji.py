from bs4 import BeautifulSoup as bs
import requests
import pandas as pd
from cmath import nan
import datetime
import pypyodbc
from itopya import *

class sinerji():
    
    pclistsinerji=[]
    for i in range(10):
        html_textisinerji=requests.get("https://www.sinerji.gen.tr/oyun-bilgisayari-c-2116?px="+str(i)).text
        soupsinerji= bs(html_textisinerji, "lxml")
        url="https://www.sinerji.gen.tr"
        try:
            hepsisinerji=soupsinerji.find("section", class_=("row productList"))
            pcs=hepsisinerji.find_all("article", class_="product")
            for pc in pcs:
                pclink=pc.find("a", href=True).get("href")
                link=url+pclink
                price=pc.find("span", class_="price").text.strip().replace("\n","").split("₺")[1]
                pcname=pc.find("div", class_="title").find("a", href=True).text.split(" ")[1:-2]
                pcname=" ".join(pcname)
                if "SSD" in pcname:
                    gpu=pcname.split("SSD")[1].strip()
                    if "GB" in gpu:
                        gpu=gpu.split(" ")[0]
                        pcd={
                        "name":pcname.split(" ")[0],
                        "gpu":gpu,
                        "price":"{:.3f}".format(float(price)),
                        "link":link,
                        }
                        pclistsinerji.append(pcd)
                    else:
                        pcd={
                        "name":pcname.split(" ")[0],
                        "gpu":gpu,
                        "price":"{:.3f}".format(float(price)),
                        "link":link,
                        }
                        pclistsinerji.append(pcd)
                else:
                    pcd={
                    "name":pcname.split(" ")[0],
                    "gpu":nan,
                    "price":"{:.3f}".format(float(price)),
                    "link":link,
                    }
                    pclistsinerji.append(pcd)
        except:
            break

    for i in pclistsinerji: 
        if i["gpu"]=="Oyun":
            i["gpu"]="nan"

    thegpulist=itopya.newgpulist
    for i in thegpulist:
        try:
           g=i["gpu"].split(" ")
           i["gpu"]="".join(g)
           #thegpulist.append(i)
        except:
           continue

    uniquegpus=[]
    for i in pclistsinerji:
        if i["gpu"] not in uniquegpus:
            uniquegpus.append(i["gpu"])
    if nan in uniquegpus:
        uniquegpus.remove(nan)
    uniquegpus.sort(reverse=True)
    
    for i in uniquegpus:
        if i not in thegpulist:
            thegpulist.append

    thegpulist.sort(reverse=True)

    db = pypyodbc.connect(
    "Driver={SQL Server};"
    "Server=DESKTOP-ADILVNP\SQLEXPRESS;"
    "Database=test;"
    "Trusted_Connection=True;"
    )
    cursor=db.cursor()
    try:
        cursor.execute("""
        CREATE TABLE sinerji (
            PCId int NOT NULL IDENTITY PRIMARY KEY,
            name varchar(255),
            gpu varchar(255),
            price nvarchar(255),
            link varchar(255)
        );
        """)
        
    except:
        cursor.execute("""
        DROP TABLE sinerji;
        CREATE TABLE sinerji (
            PCId int NOT NULL IDENTITY PRIMARY KEY,
            name varchar(255),
            gpu varchar(255),
            price nvarchar(255),
            link varchar(255)
        );
        """)
        
    insert_statement = """
    INSERT INTO sinerji
    VALUES (?, ?, ?, ?)
    """


    for i in pclistsinerji:
        try:
            cursor.execute(insert_statement, [i["name"],i["gpu"],i["price"],i["link"]])        
        except:
            print(i)
    print('records inserted successfully sinerji')
    cursor.commit()
    db.close()
