
import { useEffect, useMemo, useRef, useState } from 'react';
import { createRoot } from 'react-dom/client';
import {
  Bar, BarChart, ResponsiveContainer, Tooltip, XAxis, YAxis
} from 'recharts';
import {
  PackageOpen, Scale, Search, ShoppingBag, Layers3,
  ArrowRight, ChevronLeft, ChevronRight, SlidersHorizontal,
  Check, Database, Star
} from 'lucide-react';
import './style.css';

const API = import.meta.env.VITE_API_URL || 'http://localhost:8088/api';

type Mode = 'original' | 'thb';

type Item = {
  external_fact_key: number;
  product_name: string;
  platform_name: string;
  currency: string;
  price: number | null;
  rating: number | null;
  review_count: number | null;
  collected_at: string;
};

type Rate = {
  rate: string;
  rate_date: string;
  source: string;
};

type Amazon = {
  product_key: number;
  source_product_id: string;
  product_name: string;
  category: string | null;
  brand: string | null;
};

type Rec = {
  ranked: (Item & {
    score: number;
    reasons: string[];
    price_thb?: string;
  })[];
  limitation?: string;
  conversion?: {
    source: string;
    rate_date: string;
  };
};

async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(API + path, init);
  if (!response.ok) throw Error(`API ${response.status}`);
  return response.json() as Promise<T>;
}

const marketplaces = [
  ['ebay', 'eBay'],
  ['etsy', 'Etsy'],
  ['rakuten', 'Rakuten'],
  ['stockx', 'StockX'],
  ['advice_th', 'Advice Thailand']
];

function money(value: number | null | undefined, currency: string) {
  if (value == null) return currency ? 'ไม่มีข้อมูลราคา' : 'ไม่มีข้อมูลราคา (ไม่ระบุสกุลเงิน)';
  if (value == null) return 'ไม่มีข้อมูลราคา';
  return `${Number(value).toLocaleString(undefined, {
    maximumFractionDigits: 2
  })} ${currency}`;
}

function Selector({
  mode,
  set
}: {
  mode: Mode;
  set: (value: Mode) => void;
}) {
  return (
    <label className="currency-selector">
      แสดงราคา
      <select value={mode} onChange={e => set(e.target.value as Mode)}>
        <option value="original">Original Currency</option>
        <option value="thb">Thai Baht (THB)</option>
      </select>
    </label>
  );
}

function App() {
  const [tab, setTab] = useState('ค้นหา');
  const [q, setQ] = useState('');
  const [items, setItems] = useState<Item[]>([]);
  const [selected, setSelected] = useState<Item[]>([]);
  const [compare, setCompare] = useState<Item[]>([]);
  const [platforms, setPlatforms] = useState<string[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const [mode, setMode] = useState<Mode>('original');
  const [rates, setRates] = useState<Record<string, Rate>>({});
  const [rateLoading, setRateLoading] = useState(false);
  const [rateError, setRateError] = useState('');
  const [rec, setRec] = useState<Rec>();
  const [analyzing, setAnalyzing] = useState(false);

  const [amazonQ, setAmazonQ] = useState('');
  const [amazon, setAmazon] = useState<Amazon[]>([]);
  const [amazonTotal, setAmazonTotal] = useState(0);
  const [amazonPage, setAmazonPage] = useState(1);
  const [amazonLoading, setAmazonLoading] = useState(false);
  const [amazonError, setAmazonError] = useState('');
  const [amazonSearched, setAmazonSearched] = useState(false);

  const searchRequest = useRef(0);
  const amazonRequest = useRef(0);
  const firstLoad = useRef(false);

  const search = async (
    next = platforms,
    nextPage = 1,
    nextSize = pageSize,
    scroll = false
  ) => {
    const request = ++searchRequest.current;
    setLoading(true);
    setError('');

    try {
      const params = new URLSearchParams({
        q,
        size: String(nextSize),
        page: String(nextPage)
      });

      if (next.length) params.set('platforms', next.join(','));

      const data = await api<{ items: Item[]; total: number }>(
        `/products?${params.toString()}`
      );

      if (request !== searchRequest.current) return;

      setItems(data.items);
      setTotal(data.total);
      setPage(nextPage);
      setPageSize(nextSize);

      if (scroll) {
        requestAnimationFrame(() => {
          document.getElementById('product-results')?.scrollIntoView({
            behavior: 'smooth',
            block: 'start'
          });
        });
      }
    } catch {
      if (request === searchRequest.current) {
        setError('ค้นหาสินค้าไม่สำเร็จ กรุณาลองอีกครั้ง');
      }
    } finally {
      if (request === searchRequest.current) setLoading(false);
    }
  };

  const toggle = (item: Item) => {
    setSelected(current => {
      const exists = current.some(
        x => x.external_fact_key === item.external_fact_key
      );

      if (exists) {
        return current.filter(
          x => x.external_fact_key !== item.external_fact_key
        );
      }

      if (current.length >= 5) return current;
      return [...current, item];
    });

    setRec(undefined);
  };

  const amazonSearch = async (nextPage = 1) => {
    const request = ++amazonRequest.current;
    setAmazonLoading(true);
    setAmazonError('');
    setAmazonSearched(true);

    try {
      const params = new URLSearchParams({
        q: amazonQ,
        size: '20',
        page: String(nextPage)
      });

      const data = await api<{ items: Amazon[]; total: number }>(
        `/amazon/products?${params.toString()}`
      );

      if (request !== amazonRequest.current) return;

      setAmazon(data.items);
      setAmazonTotal(data.total);
      setAmazonPage(nextPage);
    } catch {
      if (request === amazonRequest.current) {
        setAmazonError('ค้นหา Amazon Catalog ไม่สำเร็จ');
      }
    } finally {
      if (request === amazonRequest.current) setAmazonLoading(false);
    }
  };

  // โหลดสินค้าทุก Marketplace เมื่อเปิดเว็บ
  useEffect(() => {
    if (firstLoad.current) return;
    firstLoad.current = true;
    search([]);
  }, []);

  // โหลดข้อมูลเปรียบเทียบ
  useEffect(() => {
    if (selected.length < 2) {
      setCompare([]);
      return;
    }

    let cancelled = false;

    api<Item[]>(
      `/products/compare?ids=${selected
        .map(x => x.external_fact_key)
        .join(',')}`
    )
      .then(data => {
        if (!cancelled) setCompare(data);
      })
      .catch(() => {
        if (!cancelled) setError('โหลดข้อมูลเปรียบเทียบไม่สำเร็จ');
      });

    return () => {
      cancelled = true;
    };
  }, [selected]);

  // โหลดอัตราแลกเปลี่ยน
  useEffect(() => {
    if (mode !== 'thb' || !compare.length) {
      setRates({});
      setRateError('');
      return;
    }

    let cancelled = false;
    const currencies = [
      ...new Set(
        compare.filter(x => x.price != null).map(x => x.currency)
      )
    ];

    setRateLoading(true);
    setRateError('');
    setRates({});

    Promise.all(
      currencies.map(async currency => [
        currency,
        await api<Rate>(
          `/exchange-rates?base=${encodeURIComponent(currency)}&target=THB`
        )
      ] as const)
    )
      .then(values => {
        if (cancelled) return;

        const next = Object.fromEntries(values);
        const dates = new Set(
          Object.values(next).map(x => x.rate_date)
        );

        if (dates.size > 1) {
          setRateError('อัตราแลกเปลี่ยนมีวันอ้างอิงไม่ตรงกัน');
        } else {
          setRates(next);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setRateError(
            'ไม่สามารถรับอัตราแลกเปลี่ยนจริงได้ — แสดงเฉพาะราคาเดิม'
          );
        }
      })
      .finally(() => {
        if (!cancelled) setRateLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [mode, compare]);

  const groups = useMemo(() => {
    const result: Record<string, Item[]> = {};

    compare.filter(x => x.price != null).forEach(x => {
      (result[x.currency] ??= []).push(x);
    });

    return Object.entries(result);
  }, [compare]);

  const thb = useMemo(
    () =>
      compare
        .filter(x => x.price != null && rates[x.currency])
        .map(x => ({
          ...x,
          thb: Number(x.price) * Number(rates[x.currency].rate),
          rate: rates[x.currency]
        })),
    [compare, rates]
  );

  const analyze = async () => {
    setAnalyzing(true);
    setError('');
    setRec(undefined);

    try {
      const result = await api<Rec>('/recommendations', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          fact_ids: selected.map(x => x.external_fact_key),
          priority: 'balanced',
          display_currency: mode
        })
      });

      setRec(result);
    } catch {
      setError('วิเคราะห์ความคุ้มค่าไม่สำเร็จ');
    } finally {
      setAnalyzing(false);
    }
  };

  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  const amazonPages = Math.max(1, Math.ceil(amazonTotal / 20));

  return (
    <main>
      <header>
        <div className="brand">
          <span className="brand-icon">
            <PackageOpen size={23} />
          </span>
          <span>
            Smart<span className="brand-accent">Commerce</span>
            <small>PRODUCT INTELLIGENCE</small>
          </span>
        </div>

        <nav>
          {['ค้นหา', 'เปรียบเทียบ', 'คำแนะนำ', 'Amazon Catalog'].map(name => (
            <button
              key={name}
              className={tab === name ? 'active' : ''}
              onClick={() => setTab(name)}
            >
              {name}
              {name === 'เปรียบเทียบ' && selected.length > 0 && (
                <span className="nav-count">{selected.length}</span>
              )}
            </button>
          ))}
        </nav>
      </header>

      <section className="hero">
        <div className="hero-inner">
          <p className="eyebrow">
            <Database size={14} />
            WAREHOUSE-POWERED PRODUCT ANALYTICS
          </p>

          <h1>
            ค้นหาสินค้าที่ใช่
            <span> เปรียบเทียบอย่างมั่นใจ</span>
          </h1>

          <p className="hero-description">
            สำรวจข้อมูลสินค้าและราคาจาก Marketplace
            พร้อมเครื่องมือเปรียบเทียบเพื่อช่วยตัดสินใจ
          </p>

          {tab === 'ค้นหา' && (
            <>
              <form
                className="search"
                onSubmit={e => {
                  e.preventDefault();
                  search();
                }}
              >
                <Search size={19} className="search-icon" />
                <input
                  value={q}
                  onChange={e => setQ(e.target.value)}
                  placeholder="ค้นหาชื่อสินค้า แบรนด์ หรือหมวดหมู่..."
                />
                <button type="submit" disabled={loading}>
                  ค้นหาสินค้า <ArrowRight size={17} />
                </button>
              </form>

              <div className="filters">
                <b>
                  <SlidersHorizontal size={15} />
                  Marketplace
                </b>

                {marketplaces.map(([id, label]) => (
                  <label key={id}>
                    <input
                      type="checkbox"
                      checked={platforms.includes(id)}
                      onChange={() => {
                        const next = platforms.includes(id)
                          ? platforms.filter(x => x !== id)
                          : [...platforms, id];

                        setPlatforms(next);
                        search(next);
                      }}
                    />
                    {label}
                  </label>
                ))}

                <button
                  className="clear-filter"
                  onClick={() => {
                    setPlatforms([]);
                    search([]);
                  }}
                >
                  ล้างตัวกรอง
                </button>
              </div>

              <p className="hero-total">
                <Check size={15} />
                {loading
                  ? 'กำลังค้นหาสินค้า...'
                  : `พบ ${total.toLocaleString()} รายการ`}
              </p>
            </>
          )}

          {error && <p className="error">{error}</p>}
        </div>

        <div className="hero-art" aria-hidden="true">
          <div className="hero-circle circle-one" />
          <div className="hero-circle circle-two" />
          <div className="hero-symbol">
            <ShoppingBag size={56} />
          </div>
        </div>
      </section>

      {tab === 'ค้นหา' && (
        <div id="product-results">
          <div className="results-toolbar">
            <div className="results-heading">
              <span className="results-icon">
                <Layers3 size={20} />
              </span>
              <div>
                <h2>รายการสินค้า Marketplace</h2>
                <span className="results-count">
                  {total > 0
                    ? `แสดง ${(page - 1) * pageSize + 1}–${Math.min(
                        page * pageSize, total
                      )} จาก ${total.toLocaleString()} รายการ`
                    : 'ยังไม่มีรายการสินค้า'}
                </span>
              </div>
            </div>

            <label className="page-size">
              แสดงต่อหน้า
              <select
                value={pageSize}
                disabled={loading}
                onChange={e =>
                  search(platforms, 1, Number(e.target.value))
                }
              >
                <option value={20}>20 รายการ</option>
                <option value={50}>50 รายการ</option>
              </select>
            </label>
          </div>

          <section className="grid">
            {loading ? (
              <div className="loading-state">
                <span className="spinner" />
                กำลังโหลดข้อมูลสินค้า...
              </div>
            ) : items.length ? (
              items.map(item => {
                const isSelected = selected.some(
                  x => x.external_fact_key === item.external_fact_key
                );

                return (
                  <article
                    className="card product-card"
                    key={item.external_fact_key}
                  >
                    <div className="card-top">
                      <small className="platform-badge">
                        {item.platform_name}
                      </small>
                      {item.rating != null && (
                        <span className="rating">
                          <Star size={13} fill="currentColor" />
                          {Number(item.rating).toFixed(1)}
                        </span>
                      )}
                    </div>

                    <h3 title={item.product_name}>
                      {item.product_name}
                    </h3>

                    <div className="product-spacer" />

                    {item.review_count != null && (
                      <p className="review-count">
                        {Number(item.review_count).toLocaleString()} รีวิว
                      </p>
                    )}

                    <div className="price-caption">
                      ราคาที่บันทึกไว้
                    </div>
                    <p className="product-price">
                      {money(item.price, item.currency)}
                    </p>

                    <button
                      className={
                        isSelected
                          ? 'compare-button selected'
                          : 'compare-button'
                      }
                      onClick={() => toggle(item)}
                    >
                      {isSelected ? (
                        <>
                          <Check size={15} />
                          เลือกแล้ว · นำออก
                        </>
                      ) : (
                        <>
                          <Scale size={15} />
                          เพิ่มเพื่อเปรียบเทียบ
                        </>
                      )}
                    </button>
                  </article>
                );
              })
            ) : (
              <p className="notice empty-results">
                ไม่พบสินค้าที่ตรงกับคำค้นหาหรือตัวกรอง
              </p>
            )}
          </section>

          {total > pageSize && (
            <div className="pagination">
              <button
                disabled={loading || page === 1}
                onClick={() =>
                  search(platforms, page - 1, pageSize, true)
                }
              >
                <ChevronLeft size={17} /> ก่อนหน้า
              </button>

              <span>
                หน้า <b>{page.toLocaleString()}</b> จาก{' '}
                {totalPages.toLocaleString()}
              </span>

              <button
                disabled={loading || page >= totalPages}
                onClick={() =>
                  search(platforms, page + 1, pageSize, true)
                }
              >
                ถัดไป <ChevronRight size={17} />
              </button>
            </div>
          )}
        </div>
      )}

      {tab === 'เปรียบเทียบ' && (
        <section className="content-panel">
          <div className="section-head">
            <h2><Scale /> เปรียบเทียบ ({selected.length}/5)</h2>
            <Selector mode={mode} set={setMode} />
          </div>

          {selected.length < 2 ? (
            <p className="notice">
              เลือกสินค้าอย่างน้อย 2 รายการจากหน้าค้นหา
            </p>
          ) : (
            <>
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>สินค้า</th>
                      <th>Marketplace</th>
                      <th>ราคา</th>
                      <th>เก็บข้อมูล</th>
                      <th>จัดการ</th>
                    </tr>
                  </thead>
                  <tbody>
                    {compare.map(item => (
                      <tr key={item.external_fact_key}>
                        <td>{item.product_name}</td>
                        <td>{item.platform_name}</td>
                        <td>
                          {money(item.price, item.currency)}
                          {mode === 'thb' &&
                            rates[item.currency] &&
                            item.price != null && (
                              <>
                                <br />
                                <b>
                                  ≈ {money(
                                    Number(item.price) *
                                      Number(rates[item.currency].rate),
                                    'THB'
                                  )}
                                </b>
                              </>
                            )}
                        </td>
                        <td>
                          {item.collected_at
                            ? new Date(item.collected_at)
                                .toLocaleString('th-TH')
                            : 'ไม่ระบุ'}
                        </td>
                        <td>
                          <button onClick={() => toggle(item)}>
                            นำออก
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              {mode === 'thb' ? (
                <>
                  {rateLoading && (
                    <p className="notice">
                      กำลังโหลดอัตราแลกเปลี่ยน...
                    </p>
                  )}
                  {rateError && (
                    <p className="error">{rateError}</p>
                  )}
                  {!rateLoading && !rateError && thb.length > 0 && (
                    <div className="chart">
                      <h3>กราฟเปรียบเทียบราคา THB</h3>
                      <ResponsiveContainer width="100%" height={320}>
                        <BarChart
                          data={thb.map(x => ({
                            name: x.product_name.slice(0, 24) + '…',
                            thb: x.thb
                          }))}
                        >
                          <XAxis
                            dataKey="name"
                            angle={-20}
                            textAnchor="end"
                            height={85}
                          />
                          <YAxis />
                          <Tooltip />
                          <Bar dataKey="thb" fill="#635bf2" />
                        </BarChart>
                      </ResponsiveContainer>
                      <p className="notice">
                        อัตราอ้างอิง: {thb[0]?.rate.source} ·{' '}
                        {thb[0]?.rate.rate_date}
                        {' '}— ราคา THB เป็นค่าประมาณ
                      </p>
                    </div>
                  )}
                </>
              ) : (
                groups.map(([currency, data]) => (
                  <div className="chart" key={currency}>
                    <h3>ราคาในสกุล {currency}</h3>
                    <ResponsiveContainer width="100%" height={280}>
                      <BarChart
                        data={data.map(x => ({
                          name: x.product_name.slice(0, 20) + '…',
                          price: x.price
                        }))}
                      >
                        <XAxis dataKey="name" />
                        <YAxis />
                        <Tooltip />
                        <Bar dataKey="price" fill="#635bf2" />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                ))
              )}
            </>
          )}
        </section>
      )}

      {tab === 'คำแนะนำ' && (
        <section className="content-panel">
          <div className="section-head">
            <h2>คำแนะนำความคุ้มค่า</h2>
            <Selector mode={mode} set={setMode} />
          </div>

          <p className="section-description">
            Explainable Rule-Based Recommendation — ไม่ใช่ Machine Learning
          </p>

          {selected.length < 2 ? (
            <p className="notice">กรุณาเลือกสินค้า 2–5 รายการก่อน</p>
          ) : (
            <button
              className="primary-btn"
              disabled={
                analyzing ||
                (mode === 'thb' && (rateLoading || !!rateError))
              }
              onClick={analyze}
            >
              {analyzing
                ? 'กำลังวิเคราะห์...'
                : 'วิเคราะห์ความคุ้มค่า'}
            </button>
          )}

          {rec?.limitation && (
            <p className="notice">{rec.limitation}</p>
          )}
          {rec?.conversion && (
            <p className="notice">
              อัตราอ้างอิง: {rec.conversion.source} ·{' '}
              {rec.conversion.rate_date}
            </p>
          )}

          {rec?.ranked.map((item, index) => (
            <article className="card rank" key={item.external_fact_key}>
              <b>อันดับ {index + 1} · คะแนน {item.score}</b>
              <h3>{item.product_name}</h3>
              <p>ราคาเดิม: {money(item.price, item.currency)}</p>
              {mode === 'thb' && item.price_thb && (
                <p>
                  ราคา THB โดยประมาณ:{' '}
                  {Number(item.price_thb).toLocaleString(
                    undefined, { maximumFractionDigits: 2 }
                  )}
                </p>
              )}
              <p>เหตุผล: {item.reasons.join(', ')}</p>
            </article>
          ))}
        </section>
      )}

      {tab === 'Amazon Catalog' && (
        <section className="content-panel">
          <div className="section-head">
            <h2>Amazon Product Catalog</h2>
          </div>

          <p className="notice">
            Historical Product Metadata เท่านั้น — ไม่มี Current Price Snapshot
          </p>

          <form
            className="search amazon-search"
            onSubmit={e => {
              e.preventDefault();
              amazonSearch(1);
            }}
          >
            <Search size={19} className="search-icon" />
            <input
              value={amazonQ}
              onChange={e => setAmazonQ(e.target.value)}
              placeholder="ค้นหาด้วย ASIN หรือชื่อสินค้า..."
            />
            <button type="submit" disabled={amazonLoading}>
              <Search size={16} /> ค้นหา Amazon
            </button>
          </form>

          {amazonError && (
            <p className="error">{amazonError}</p>
          )}

          {amazonLoading ? (
            <div className="loading-state">
              <span className="spinner" />
              กำลังค้นหา Amazon Catalog...
            </div>
          ) : !amazonSearched ? (
            <p className="notice">
              กรอก ASIN หรือชื่อสินค้าแล้วกดค้นหา
            </p>
          ) : amazon.length === 0 ? (
            <p className="notice">
              ไม่พบสินค้า Amazon ที่ตรงกับคำค้น
            </p>
          ) : (
            <>
              <p className="catalog-count">
                พบ {amazonTotal.toLocaleString()} รายการ
              </p>

              <div className="grid">
                {amazon.map(item => (
                  <article
                    className="card amazon-card"
                    key={item.product_key}
                  >
                    <small>ASIN: {item.source_product_id}</small>
                    <h3>{item.product_name}</h3>
                    <p>
                      <b>หมวดหมู่:</b> {item.category || 'ไม่ระบุ'}
                    </p>
                    <p>
                      <b>Brand/Store:</b> {item.brand || 'ไม่ระบุ'}
                    </p>
                  </article>
                ))}
              </div>

              {amazonTotal > 20 && (
                <div className="pagination">
                  <button
                    disabled={amazonLoading || amazonPage === 1}
                    onClick={() => amazonSearch(amazonPage - 1)}
                  >
                    <ChevronLeft size={17} /> ก่อนหน้า
                  </button>
                  <span>
                    หน้า <b>{amazonPage}</b> จาก {amazonPages}
                  </span>
                  <button
                    disabled={
                      amazonLoading || amazonPage >= amazonPages
                    }
                    onClick={() => amazonSearch(amazonPage + 1)}
                  >
                    ถัดไป <ChevronRight size={17} />
                  </button>
                </div>
              )}
            </>
          )}
        </section>
      )}

      <footer className="site-footer">
        <span>Smart Commerce · Big Data Product Analytics</span>
        <span>Data from warehouse snapshots</span>
      </footer>
    </main>
  );
}

createRoot(document.getElementById('root')!).render(<App />);
