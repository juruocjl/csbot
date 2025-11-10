from pathlib import Path
import requests
import sqlite3

headers = {'ApiToken': input("Please input api: "), 'Content-Type': 'application/json'}

db = sqlite3.connect("groups.db")

def getallgoodsid():
    cursor = db.cursor()
    cursor.execute('SELECT DISTINCT goodId FROM goods_info')
    res = cursor.fetchall()
    return [a[0] for a in res]

def download_img(id: str):
    resp = requests.get(f"https://api.csqaq.com/api/v1/info/good?id={id}", headers=headers)
    imgurl = resp.json()['data']['goods_info']['img']
    hashname = resp.json()['data']['goods_info']['market_hash_name']
    imgpath = Path("goodsimg") / f"{hashname}.jpg"
    if imgpath.exists():
        return
    imgdata = requests.get(imgurl).content
    with open(imgpath, "wb") as f:
        f.write(imgdata)

for id in getallgoodsid():
    try:
        download_img(str(id))
        print(f"Downloaded image for goodId {id}")
    except Exception as e:
        print(f"Failed to download image for goodId {id}: {e}")