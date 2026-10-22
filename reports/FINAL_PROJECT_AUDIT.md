# Final Project Audit — 12 ตุลาคม 2026

## A. Executive Summary

ผลตรวจเป็นแบบ read-only จากไฟล์, Docker, Warehouse และ API จริง โดยไม่รัน pipeline และไม่แก้ข้อมูล. ระบบหลักเปิดใช้งานได้ แต่ยังไม่ควรกล่าวว่า pipeline execution ผ่านครบทุก DAG หรือ UI ผ่าน E2E เพราะไม่ได้ทดสอบสิ่งเหล่านั้นด้วย browser/trigger จริง

## B. Component Audit

| ส่วนประกอบ | สถานะ | หลักฐาน |
|---|---|---|
| Docker / network | PASS | 9 containers อยู่สถานะ Up; อยู่ `ecommerce-network` |
| Airflow scheduler/webserver | PASS (health) | scheduler/webserver Up; parse DAG ได้ 3 DAG |
| Spark master/worker | PASS (service) | Up และมีงาน Spark รับ/จบใน log |
| Warehouse PostgreSQL | PASS | healthy, query read-only สำเร็จ |
| FastAPI | PASS | `/health`, OpenAPI และ endpoint หลักตอบ 200 |
| React/Vite | PASS (HTTP/build ก่อนหน้า) | `localhost:5174` ตอบ 200; browser E2E ไม่ได้ทดสอบ |
| Power BI PBIX | NOT VERIFIED | ไม่เปิด/แก้ PBIX; ตรวจได้เฉพาะ SQL views และคู่มือ |

ทรัพยากรขณะตรวจ: Warehouse 1,004 MiB, Airflow webserver 645.5 MiB, scheduler 549.8 MiB, Spark worker 165.9 MiB. ไม่พบ restart/error บล็อกการเริ่ม service ใน log ที่ตรวจ.

## C. Warehouse Data

| Marketplace | Fact snapshots | distinct products | ช่วง collected_at | หมายเหตุ |
|---|---:|---:|---|---|
| eBay | 114,822 | 105,958 | 2026-10-06 ถึง 2026-10-10 | currency ไม่ว่าง |
| Etsy | 82,683 | 82,683 | 2026-10-11 | currency ไม่ว่าง |
| Rakuten | 75,155 | 70,682 | 2026-10-04 ถึง 2026-10-11 | currency ไม่ว่าง |
| StockX | 3,247 | 2,996 | 2026-10-09 ถึง 2026-10-10 | currency ว่าง 3,247 แถว |
| Advice Thailand | 196 | 196 | 2026-10-11 | currency ไม่ว่าง |
| รวม | 276,103 | — | — | Price snapshots เท่านั้น |

Amazon `amazon_2023` ใน `dim_product`: 1,722,496 records, 1,722,496 ASIN ไม่ซ้ำ, ASIN ว่าง 0. เป็น historical metadata ไม่ใช่ price snapshot. พบ orphan product/platform/date key = 0, negative price = 0. มี PK/unique FK/index หลักตาม `pg_indexes`.

## D. Functional Testing

| การทดสอบ | ผล |
|---|---|
| `/api/health`, summary, platforms, product search/filter, compare | PASS (HTTP 200) |
| compare invalid `ids=bad` | PASS: HTTP 422 validation |
| Amazon ASIN `B00E5U3YEK` | PASS: พบ LG Electronics 1 record |
| Amazon ชื่อ `LG`, page 2 | PASS: total 34,921, คืน 20 records |
| USD→THB | PASS: `33.579`, date `2026-10-11`, Frankfurter v2 |
| Recommendation THB USD+GBP | PASS (API ที่ทดสอบก่อน audit) |
| Frontend browser interactions/chart/tooltip/responsive | NOT TESTED: ไม่มี browser automation |
| Full DAG execution / retry | NOT TESTED: ไม่ trigger เพราะอาจเขียนข้อมูล |

## E. Bugs / Findings

| ID | ระดับ | ข้อเท็จจริง / ผลกระทบ |
|---|---|---|
| DATA-001 | Major | StockX ทุก fact record มี currency เป็น NULL; ห้ามนำไปคำนวณ/เปรียบเทียบข้ามสกุลเงิน |
| PIPE-001 | Warning | `airflow dags list-runs -d 01_ingest_data_dag` ใช้ id ไม่ตรง; DAG จริงคือ `01_ingest_external_data` จึงไม่ยืนยัน historical run ของชื่อที่ร้องขอได้ |
| SPARK-001 | Warning | Spark master log มี WARN `unknown executor` หลังงาน InspectAmazonHistoricalMetadata unregister; งานจบ/worker cleanup ต่อมา จึงยังไม่ยืนยัน failure ของ pipeline |
| UI-001 | Limitation | Browser E2E/console/mobile ไม่ได้ทดสอบใน environment นี้ |

## F. Fixes Applied / Regression

งานก่อน audit แก้ Amazon Catalog ใน `web/frontend/src.tsx`: ค้นหา ASIN/ชื่อ, loading/error/empty, pagination และแสดง metadata จาก `/api/amazon/products`; audit ยืนยัน API schema/ASIN/page 2 ข้างต้น. ไม่แก้ pipeline, schema หรือ PBIX ใน audit นี้.

## G. Remaining Issues

- Critical: ไม่มีที่ยืนยันได้จากการตรวจนี้
- Major: แก้/อธิบาย StockX currency NULL ก่อนนำเสนอกราฟหรือ THB comparison ที่รวม StockX
- Major: ตรวจ DAG historical runs ด้วย DAG id ที่ถูกต้อง และตรวจ task/log ก่อนกล่าวว่า pipeline execution ผ่าน
- Minor: Vite bundle >500 kB warning; พิจารณา code split ภายหลัง
- Minor: ต้องทดสอบ Power BI Desktop, browser responsive และ E2E ด้วยมือ/เครื่องมือที่รองรับ

## H. Power BI Review

`sql/04_powerbi_views.sql` มี `vw_powerbi_marketplace_snapshot` ที่คง `currency` ไว้ใน grain และ exclude Amazon จาก fact snapshot ถูกต้อง. คู่มือ `dashboards/POWER_BI_REFRESH_GUIDE.md` ระบุไม่เฉลี่ย currency ข้ามกัน และให้ใช้ Amazon เฉพาะ product/brand/category. PBIX `dashboards/ecommerce_analytics.pbix` มีอยู่แต่ NOT VERIFIED: ให้ตรวจใน Power BI Desktop ว่า card platform นับเฉพาะ fact-priced marketplace, ราคา average มี currency legend/filter, และ title ระบุ Amazon historical metadata.

## I. Presentation Checklist

- เปิด `docker compose up -d` เฉพาะเมื่อจำเป็น; ห้าม `down -v`
- Airflow `http://localhost:8190`, Spark `http://localhost:8181`, Web `http://localhost:5174`, API docs `http://localhost:8088/docs`
- อธิบายว่าเป็น batch/snapshot ไม่ใช่ real-time
- สาธิต 5 marketplace จาก fact; Amazon ASIN แยกเป็น historical catalog
- หลีกเลี่ยง StockX ใน THB demo จน currency quality ได้รับการยืนยัน

## J. Final Verdict

**READY WITH LIMITATIONS** — infrastructure, Warehouse integrity หลัก, API และ Amazon catalog API พร้อมสาธิตได้; แต่ต้องเปิดเผย StockX currency NULL, ผล DAG execution ยังไม่ verified, PBIX/browser E2E ยังไม่ verified ก่อนนำเสนอ.
