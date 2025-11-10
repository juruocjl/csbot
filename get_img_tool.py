from pathlib import Path
import requests
import sqlite3
import time

headers = {'ApiToken': input("Please input api: "), 'Content-Type': 'application/json'}

db = sqlite3.connect("groups.db")

def getallgoodsid():
    cursor = db.cursor()
    cursor.execute('SELECT DISTINCT goodId FROM goods_info')
    res = cursor.fetchall()
    return [a[0] for a in res]

def download_img(id: str):
    resp = requests.get(f"https://api.csqaq.com/api/v1/info/good?id={id}", headers=headers)
    try:
        imgurl = resp.json()['data']['goods_info']['img']
        imgpath = Path("goodsimg") / f"{id}.jpg"
        if imgpath.exists():
            return
        imgdata = requests.get(imgurl).content
        with open(imgpath, "wb") as f:
            f.write(imgdata)
    except:
        print(f"Failed to download image for id {id}")
        print(resp.text)

for id in getallgoodsid():
    download_img(str(id))
    time.sleep(1.2)