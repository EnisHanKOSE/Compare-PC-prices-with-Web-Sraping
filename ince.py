from bs4 import BeautifulSoup as bs
import requests
import pandas as pd
import datetime
from cmath import nan
import pypyodbc

def final_ince():
    pclistince=[]
    def getincehesap():    
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
        return
        
    getincehesap()
    unique_gpus=[]
    for i in pclistince:
        if i["gpu"] not in unique_gpus:
            unique_gpus.append(i["gpu"])
    try:
        unique_gpus.remove(nan)
    except:
        print("No nan")
    try:
        unique_gpus.sort(reverse=True)
    except:
        print(unique_gpus)
    f = open("incegpu.txt", "w")
    with open(r'incegpu.txt', 'w') as fp:
        for item in unique_gpus:
            # write each item on a new line
            fp.write("%s\n" % item)
    """ dfince = pd.DataFrame(pclistince) """
    pclistinceg=[]
    for i in pclistince:
        try:
            g=i["gpu"].split(" ")
            i["gpu"]="".join(g)
            pclistinceg.append(i)
        except:
            continue
    """ dfinceg = pd.DataFrame(pclistinceg)
    dfinceg.to_csv("inceg.csv", sep='\t', encoding='utf-8')
    dfinceg.to_csv("inceg_csv", sep='\t', encoding='utf-8') """
    """ unique_gpus = list(set(dfince["gpu"])) """
    unique_gpusg=[]
    for i in pclistinceg:
        if i["gpu"] not in unique_gpusg:
            unique_gpusg.append(i["gpu"])
    unique_gpusg.sort(reverse=True)
    f = open("incegpug.txt", "w")
    with open(r'incegpug.txt', 'w') as fp:
        for item in unique_gpusg:
            # write each item on a new line
            fp.write("%s\n" % item)
    nvidialist=[]
    amdlist=[]
    for i in pclistince:
        if  ("nan" not in str(i["gpu"])) and ("RX" not in str(i["gpu"])):
            nvidialist.append(i)
        elif ("nan" not in str(i["gpu"])) and ("RX" in str(i["gpu"])):
            amdlist.append(i)
    sortednvidia = sorted(nvidialist, key=lambda d: d['price'])
    sortedamd = sorted(amdlist, key=lambda d: d['price'])
    """ dfince.to_csv("incehesap.csv", sep='\t', encoding='utf-8')
    dfince.to_csv("incehesap_csv", sep='\t', encoding='utf-8') """
    db = pypyodbc.connect(
    "Driver={SQL Server};"
    "Server=DESKTOP-ADILVNP\SQLEXPRESS;"
    "Database=test;"
    "Trusted_Connection=True;"
    )
    cursor=db.cursor()
    try:
        cursor.execute("""
        CREATE TABLE test2 (
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
        DROP TABLE test2;
        CREATE TABLE test2 (
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
    INSERT INTO test2
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """


    for i in pclistinceg:
        cursor.execute(insert_statement, [i["name"],i["cpu"],i["ram"],i["gpu"],i["storage"],i["price"],i["link"]])        

    print('records inserted successfully')
    cursor.commit()
    db.close()
    return