from bs4 import BeautifulSoup as bs
import requests
import pandas as pd
from cmath import nan
import datetime
import pypyodbc

def final_sinerji():
    pclistsinerji=[]
    def getsinerji():
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
                            "name":pcname,
                            "gpu":gpu,
                            "price":"{:.3f}".format(float(price)),
                            "link":link,
                            }
                            pclistsinerji.append(pcd)
                        else:
                            pcd={
                            "name":pcname,
                            "gpu":gpu,
                            "price":"{:.3f}".format(float(price)),
                            "link":link,
                            }
                            pclistsinerji.append(pcd)
                    else:
                        pcd={
                        "name":pcname,
                        "gpu":nan,
                        "price":"{:.3f}".format(float(price)),
                        "link":link,
                        }
                        pclistsinerji.append(pcd)
            except:
                break

    getsinerji()

    file1 = open('thegpu.txt', 'r')
    Lines = file1.readlines()
    thegpulist=[]
    for i in Lines:
        i=i.strip().split(" ")
        i="".join(i)
        thegpulist.append(i)
    thegpulist.sort(reverse=True)

    """ dfsinerji = pd.DataFrame(pclistsinerji) """
    unique_gpus=[]
    for i in pclistsinerji:
        if i["gpu"] not in unique_gpus:
            unique_gpus.append(i["gpu"])
    if nan in unique_gpus:
        unique_gpus.remove(nan)
    unique_gpus.sort(reverse=True)
    with open(r'sinerji.txt', 'w') as fp:
        for item in unique_gpus:
            fp.write("%s\n" % item)

    file1 = open('sinerji.txt', 'r')
    Lines = file1.readlines()
    sinerjilist=[]
    for i in Lines:
        sinerjilist.append(i.strip())
    sinerjilist.sort(reverse=True)

    for i in sinerjilist:
        if i not in thegpulist:
            thegpulist.append(i)

    thegpulist.sort(reverse=True)
    with open(r'thegpug.txt', 'w') as fp:
        for item in thegpulist:
            fp.write("%s\n" % item)

    """ dfsinerji.to_csv("sinerji.csv", sep='\t', encoding='utf-8')
    dfsinerji.to_csv("sinerji_csv", sep='\t', encoding='utf-8') """
    db = pypyodbc.connect(
    "Driver={SQL Server};"
    "Server=DESKTOP-ADILVNP\SQLEXPRESS;"
    "Database=test;"
    "Trusted_Connection=True;"
    )
    cursor=db.cursor()
    try:
        cursor.execute("""
        CREATE TABLE test4 (
            PCId int NOT NULL IDENTITY PRIMARY KEY,
            name varchar(255),
            gpu varchar(255),
            price nvarchar(255),
            link varchar(255)
        );
        """)
    except:
        cursor.execute("""
        DROP TABLE test2;
        CREATE TABLE test2 (
            PCId int NOT NULL IDENTITY PRIMARY KEY,
            name varchar(255),
            gpu varchar(255),
            price nvarchar(255),
            link varchar(255)
        );
        """)
    insert_statement = """
    INSERT INTO test2
    VALUES (?, ?, ?, ?)
    """


    for i in pclistsinerji:
        try:
            cursor.execute(insert_statement, [i["name"],i["gpu"],i["price"],i["link"]])        
        except:
            print(i)
    print('records inserted successfully')
    cursor.commit()
    db.close()
    return