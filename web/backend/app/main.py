import os
import time
import json
from decimal import Decimal
from urllib.request import Request, urlopen
from datetime import date
from typing import Literal

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool

DB_URL = os.environ["DATABASE_URL"]
engine = create_engine(DB_URL, poolclass=QueuePool, pool_size=5, max_overflow=5, pool_pre_ping=True)
app = FastAPI(title="Smart Commerce API", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=os.getenv("CORS_ORIGINS", "http://localhost:5174").split(","), allow_methods=["GET","POST"], allow_headers=["*"])
RATE_CACHE: dict[tuple[str,str], tuple[float,dict]] = {}

def rows(sql, params=None):
    with engine.connect() as c:
        return [dict(r._mapping) for r in c.execute(text(sql), params or {})]

@app.get("/api/health")
def health():
    rows("SELECT 1")
    return {"status": "ok"}

@app.get("/api/platforms")
def platforms():
    return rows("SELECT platform_name, platform_type FROM dim_platform WHERE platform_type <> 'internal' ORDER BY platform_name")

@app.get("/api/exchange-rates")
def exchange_rates(base: str = Query(..., min_length=3, max_length=3), target: str = Query("THB", min_length=3, max_length=3)):
    base,target=base.upper(),target.upper()
    if base == target:
        return {"base":base,"target":target,"rate":"1","rate_date":str(date.today()),"source":"identity (no conversion)","cached":False}
    key=(base,target); cached=RATE_CACHE.get(key)
    if cached and time.time()-cached[0] < 3600: return {**cached[1],"cached":True}
    try:
        request = Request(
            f"https://api.frankfurter.dev/v2/rate/{base.lower()}/{target.lower()}",
            headers={"User-Agent": "ecommerce-bigdata-pipeline/1.0"},
        )
        with urlopen(request, timeout=5) as response:
            data=json.load(response)
        result={"base":base,"target":target,"rate":str(Decimal(str(data["rate"]))),"rate_date":data.get("date"),"source":"Frankfurter v2"}
        RATE_CACHE[key]=(time.time(),result); return {**result,"cached":False}
    except Exception as exc:
        if cached: return {**cached[1],"cached":True,"warning":"ใช้ cache เนื่องจาก Frankfurter ไม่พร้อม"}
        raise HTTPException(503,"Exchange rate unavailable; original price retained") from exc

@app.get("/api/analytics/summary")
def summary():
    return rows("""SELECT COUNT(*) AS snapshot_records, COUNT(DISTINCT f.product_key) AS searchable_products,
      COUNT(DISTINCT f.platform_key) AS priced_platforms, MAX(f.collected_at) AS latest_collected_at
      FROM fact_external_product f""")[0]

PRODUCT_SQL = """WITH latest AS (SELECT DISTINCT ON (f.product_key,f.platform_key) f.* FROM fact_external_product f
 ORDER BY f.product_key,f.platform_key,f.collected_at DESC NULLS LAST,f.external_fact_key DESC)
 SELECT l.external_fact_key,l.product_key,p.product_name,p.category,p.brand,pl.platform_name,l.currency,l.price,l.rating,l.review_count,l.product_url,l.collected_at,d.full_date
 FROM latest l JOIN dim_product p ON p.product_key=l.product_key JOIN dim_platform pl ON pl.platform_key=l.platform_key JOIN dim_date d ON d.date_key=l.date_key
 WHERE (:q IS NULL OR p.product_name ILIKE :like OR COALESCE(p.brand,'') ILIKE :like OR COALESCE(p.category,'') ILIKE :like)
 AND (:platform IS NULL OR pl.platform_name=:platform) AND (:currency IS NULL OR l.currency=:currency)
 AND (:min_price IS NULL OR l.price >= :min_price) AND (:max_price IS NULL OR l.price <= :max_price)"""

@app.get("/api/products")
def products(q: str | None = Query(None, max_length=100), platform: str | None = None, platforms: str | None = None, currency: str | None = None,
 min_price: float | None = Query(None, ge=0), max_price: float | None = Query(None, ge=0), page: int = Query(1, ge=1), size: int = Query(20, ge=1, le=50), sort: Literal['recent','price_asc','price_desc','rating','reviews']='recent'):
    if min_price is not None and max_price is not None and min_price > max_price: raise HTTPException(422, 'min_price must not exceed max_price')
    order = {'recent':'l.collected_at DESC NULLS LAST','price_asc':'l.price ASC NULLS LAST','price_desc':'l.price DESC NULLS LAST','rating':'l.rating DESC NULLS LAST','reviews':'l.review_count DESC NULLS LAST'}[sort]
    selected=[x.strip() for x in (platforms or platform or '').split(',') if x.strip()]
    allowed={'ebay','etsy','rakuten','stockx','advice_th'}
    if any(x not in allowed for x in selected): raise HTTPException(422,'Unsupported marketplace')
    clause=' AND (:platforms_empty OR pl.platform_name = ANY(:platforms))'
    p={'q':q, 'like':f'%{q}%' if q else None, 'platform':platform,'platforms':selected,'platforms_empty':not selected,'currency':currency,'min_price':min_price,'max_price':max_price,'limit':size,'offset':(page-1)*size}
    data=rows(PRODUCT_SQL + clause + ' ORDER BY '+order+' LIMIT :limit OFFSET :offset',p)
    total=rows('SELECT COUNT(*) AS n FROM ('+PRODUCT_SQL + clause +') x',p)[0]['n']
    return {'items':data,'page':page,'size':size,'total':total}

@app.get("/api/products/compare")
def compare(ids: str = Query(..., pattern=r'^\d+(,\d+){1,4}$')):
    values=[int(x) for x in ids.split(',')]
    return rows("""SELECT f.external_fact_key,f.product_key,p.product_name,pl.platform_name,f.currency,f.price,f.rating,f.review_count,f.product_url,f.collected_at
      FROM fact_external_product f JOIN dim_product p USING(product_key) JOIN dim_platform pl USING(platform_key)
      WHERE f.external_fact_key = ANY(:ids) ORDER BY f.external_fact_key""", {'ids':values})

@app.get("/api/products/{product_key}")
def product(product_key: int):
    data=rows("""SELECT p.product_key,p.source_product_id,p.product_name,p.category,p.brand,pl.platform_name,f.currency,f.price,f.rating,f.review_count,f.product_url,f.collected_at
      FROM dim_product p LEFT JOIN fact_external_product f ON f.product_key=p.product_key LEFT JOIN dim_platform pl ON pl.platform_key=f.platform_key
      WHERE p.product_key=:id ORDER BY f.collected_at DESC NULLS LAST""", {'id':product_key})
    if not data: raise HTTPException(404,'Product not found')
    return data

class RecommendationRequest(BaseModel):
    fact_ids: list[int] = Field(min_length=2,max_length=5)
    priority: Literal['balanced','price','rating']='balanced'
    display_currency: Literal['original','thb']='original'

@app.post("/api/recommendations")
def recommendations(body: RecommendationRequest):
    items=rows("SELECT f.external_fact_key,p.product_name,pl.platform_name,f.currency,f.price,f.rating,f.review_count,f.collected_at FROM fact_external_product f JOIN dim_product p USING(product_key) JOIN dim_platform pl USING(platform_key) WHERE f.external_fact_key=ANY(:ids)",{'ids':body.fact_ids})
    currencies={x['currency'] for x in items if x['price'] is not None and x['currency']}
    if not items: raise HTTPException(404,'No selected products found')
    conversion=None
    if body.display_currency == 'thb':
        if any(not x['currency'] for x in items):
            return {'ranked': [], 'limitation':'มีสินค้าที่ไม่ระบุสกุลเงิน จึงไม่จัดอันดับราคาหรือแปลงเป็น THB'}
        rate_info={}
        try:
            for currency in currencies:
                rate_info[currency]=exchange_rates(currency, 'THB')
        except HTTPException:
            return {'ranked': [], 'limitation':'ไม่สามารถรับอัตราแลกเปลี่ยนจริงได้ จึงไม่จัดอันดับราคาข้ามสกุลเงิน'}
        dates={x['rate_date'] for x in rate_info.values()}
        if len(dates)>1:
            return {'ranked': [], 'limitation':'อัตราแลกเปลี่ยนมีวันอ้างอิงไม่ตรงกัน จึงไม่จัดอันดับราคาข้ามสกุลเงิน'}
        for x in items:
            if x['price'] is not None:
                info=rate_info[x['currency']]
                x['price_thb']=str(Decimal(str(x['price'])) * Decimal(info['rate']))
        conversion={'target':'THB','rate_date':next(iter(dates), None),'source':'Frankfurter v2','rates':rate_info}
        prices=[float(x['price_thb']) for x in items if x.get('price_thb') is not None]
    else:
        if len(currencies)>1: return {'ranked': [], 'limitation':'ไม่จัดอันดับราคาเมื่อมีหลายสกุลเงิน; กรุณาเลือกสินค้าในสกุลเงินเดียวกัน'}
        prices=[float(x['price']) for x in items if x['price'] is not None]
    for x in items:
        score=0; reasons=[]
        rank_price=x.get('price_thb') if body.display_currency == 'thb' else x['price']
        if rank_price is not None and prices: score += (max(prices)-float(rank_price))/(max(prices)-min(prices) or 1) * (0.6 if body.priority=='price' else .35); reasons.append('มีราคา')
        if x['rating'] is not None: score += float(x['rating'])/5*(.6 if body.priority=='rating' else .35); reasons.append('มีคะแนนรีวิว')
        if x['review_count'] is not None: score += min(float(x['review_count'])/1000,1)*.15; reasons.append('มีจำนวนรีวิว')
        x.update(score=round(score,3), reasons=reasons or ['ข้อมูลไม่เพียงพอสำหรับคะแนน'])
    return {'ranked':sorted(items,key=lambda x:x['score'],reverse=True),'rule_based':True,'conversion':conversion}

@app.get("/api/amazon/products")
def amazon(q:str|None=Query(None,max_length=100),category:str|None=None,brand:str|None=None,page:int=Query(1,ge=1),size:int=Query(20,ge=1,le=50)):
    p={'q':q,'like':f'%{q}%' if q else None,'category':category,'brand':brand,'limit':size,'offset':(page-1)*size}
    where="source_system='amazon_2023' AND (:q IS NULL OR product_name ILIKE :like OR source_product_id ILIKE :like) AND (:category IS NULL OR category=:category) AND (:brand IS NULL OR brand=:brand)"
    data=rows('SELECT product_key,source_product_id,product_name,category,brand FROM dim_product WHERE '+where+' ORDER BY product_key LIMIT :limit OFFSET :offset',p)
    total=rows('SELECT COUNT(*) n FROM dim_product WHERE '+where,p)[0]['n']
    return {'items':data,'page':page,'size':size,'total':total,'source_note':'Historical Product Metadata; not a current price snapshot'}
