import { useEffect, useState } from "react";
import uvLogo from "./assets/uv-logo.png";

export default function App() {
  const API_BASE = (import.meta.env.VITE_API_BASE_URL || "").replace(/\/$/, "");
  const [lang, setLang] = useState("it");
  const [dbRows, setDbRows] = useState([]);
  const [dbCols, setDbCols] = useState([]);
  const [dbSearch, setDbSearch] = useState("");
  const [dbPage, setDbPage] = useState(0);
  const [dbExpanded, setDbExpanded] = useState(false);
  const [dbLoading, setDbLoading] = useState(false);
  const [dbFilters, setDbFilters] = useState({});
  const [filterOptions, setFilterOptions] = useState({});
  const [statsData, setStatsData] = useState({
    totalsByYear: [],
    topSectors: [],
    topCities: [],
    roundsByYear: [],
    checks: {}
  });
  const [chatQ, setChatQ] = useState("");
  const [chatA, setChatA] = useState("");
  const [chatLoading, setChatLoading] = useState(false);
  const [runLoading, setRunLoading] = useState(false);
  const [runStatus, setRunStatus] = useState("");

  const t = lang === "it" ? {
    badge: "TWIS → Osservatorio VC",
    heroTitle: "Round italiani dal 2022 ad oggi.\nAggiornamento settimanale.",
    heroBody: "Ogni lunedì alle 09:10, l’automazione legge TWIS, estrae “The Money” ed aggiorna il database.",
    dbTitle: "Database rounds italiani",
    dbSubtitle: "Replica digitale dell’Excel, aggiornata automaticamente ogni lunedì.",
    searchPlaceholder: "Cerca per azienda, settore, investor...",
    search: "Cerca",
    loading: "Caricamento dati...",
    shown: "Mostrati",
    prev: "Prev",
    next: "Next",
    expand: "Espandi",
    reduce: "Riduci",
    clear: "Pulisci filtri",
    chatTitle: "Chat con il database",
    chatPlaceholder: "Chiedi qualcosa ai dati (es. quanti round in Q1 2026?)",
    ask: "Chiedi",
    analyzing: "Analizzo...",
    run: "RUN",
    running: "Esecuzione in corso...",
    runDone: "Aggiornamento completato",
    chatEmpty: "La risposta apparirà qui.",
    chartsYear: "Totale raccolto per anno (2022–2026)",
    chartsYearSub: "€M, aggiornato al dato disponibile",
    chartsSector: "Top settori per raccolto",
    chartsSectorSub: "€M complessivi",
    chartsCity: "Top città per raccolto",
    chartsCitySub: "€M complessivi",
    chartsCount: "Numero di round per anno",
    chartsCountSub: "conteggio",
    footer: "Crafted by Matteo Moscarelli"
  } : {
    badge: "TWIS → VC Observatory",
    heroTitle: "Italian rounds from 2022 to today.\nWeekly update.",
    heroBody: "Every Monday at 09:10, the automation reads TWIS, extracts “The Money” and updates the database.",
    dbTitle: "Italian rounds database",
    dbSubtitle: "Digital replica of the Excel, updated automatically every Monday.",
    searchPlaceholder: "Search by company, sector, investor...",
    search: "Search",
    loading: "Loading data...",
    shown: "Shown",
    prev: "Prev",
    next: "Next",
    expand: "Expand",
    reduce: "Collapse",
    clear: "Clear filters",
    chatTitle: "Chat with the database",
    chatPlaceholder: "Ask the data (e.g., how many rounds in Q1 2026?)",
    ask: "Ask",
    analyzing: "Analyzing...",
    run: "RUN",
    running: "Running...",
    runDone: "Update completed",
    chatEmpty: "The answer will appear here.",
    chartsYear: "Total raised by year (2022–2026)",
    chartsYearSub: "€M, updated to date",
    chartsSector: "Top sectors by raised",
    chartsSectorSub: "€M total",
    chartsCity: "Top cities by raised",
    chartsCitySub: "€M total",
    chartsCount: "Number of rounds per year",
    chartsCountSub: "count",
    footer: "Crafted by Matteo Moscarelli"
  };

  const hiddenCols = new Set(["Female founder", "Spin-off?", "FX", "Tag"]);
  const roundSizeBuckets = [
    { label: "<€1M", value: "lt:1" },
    { label: "from €1M to €3M", value: "between:1:3" },
    { label: "from €3M to €5M", value: "between:3:5" },
    { label: "from €5M to €10M", value: "between:5:10" },
    { label: "from €10M to €25M", value: "between:10:25" },
    { label: "from €25M to €100M", value: "between:25:100" },
    { label: ">€100M", value: "gt:100" }
  ];
  const loadDb = async (page = 0, search = "", filters = {}, limitOverride = null) => {
    setDbLoading(true);
    try {
      const limit = limitOverride ?? (dbExpanded ? 50 : 10);
      const offset = page * limit;
      if (Object.keys(filters).length > 0) {
        const res = await fetch(`${API_BASE}/api/rounds/query`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ filters, limit, offset }),
          cache: "no-store"
        });
        const data = await res.json();
        setDbRows(data.rows || []);
        const cols = (data.columns || [])
          .filter((c) => !hiddenCols.has(c))
          .filter((c) => data.rows?.some((r) => String(r[c] || "").trim() !== ""));
        setDbCols(cols);
        await loadFilterOptions(cols);
      } else {
        const res = await fetch(`${API_BASE}/api/rounds?limit=${limit}&offset=${offset}&search=${encodeURIComponent(search)}&t=${Date.now()}`, {
          cache: "no-store"
        });
        const data = await res.json();
        setDbRows(data.rows || []);
        const cols = (data.columns || [])
          .filter((c) => !hiddenCols.has(c))
          .filter((c) => data.rows?.some((r) => String(r[c] || "").trim() !== ""));
        setDbCols(cols);
        await loadFilterOptions(cols);
      }
    } finally {
      setDbLoading(false);
    }
  };

  const loadFilterOptions = async (cols) => {
    const next = {};
    await Promise.all(
      cols.map(async (c) => {
        try {
          if (c === "Round size (€M)") {
            next[c] = roundSizeBuckets.map((b) => b.value);
            return;
          }
          const res = await fetch(`${API_BASE}/api/rounds/distinct?col=${encodeURIComponent(c)}&t=${Date.now()}`, {
            cache: "no-store"
          });
          const data = await res.json();
          next[c] = (data.values || []).sort();
        } catch {
          next[c] = [];
        }
      })
    );
    setFilterOptions(next);
  };

  useEffect(() => {
    loadDb(0, "");
  }, []);

  const handleSearch = (e) => {
    e.preventDefault();
    setDbPage(0);
    setDbFilters({});
    loadDb(0, dbSearch, {});
  };

  const handleChat = async () => {
    if (!chatQ.trim()) return;
    setChatLoading(true);
    setChatA("Richiesta inviata...");
    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 20000);
      const res = await fetch(`${API_BASE}/api/chat`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ question: chatQ, lang }),
        signal: controller.signal
      });
      clearTimeout(timeoutId);
      const text = await res.text();
      if (!res.ok) {
        throw new Error(text || "Errore API");
      }
      let data;
      try {
        data = JSON.parse(text);
      } catch {
        data = { answer: text };
      }
      setChatA(data.answer || "Nessuna risposta.");
    } catch (err) {
      setChatA(`Errore: ${err.message}`);
    } finally {
      setChatLoading(false);
    }
  };

  const handleRun = async () => {
    setRunLoading(true);
    setRunStatus(t.running);
    try {
      const res = await fetch(`${API_BASE}/api/run`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          subject: "TWIS",
          recent_days: 30,
          rss_url: "https://dealflowit.niccolosanarico.com/feed"
        })
      });
      const data = await res.json();
      if (!res.ok || data.status === "Error") {
        throw new Error(data.error || "Run failed");
      }
      setRunStatus(`${t.runDone}: ${data.rows || 0} righe`);
      setDbPage(0);
      setDbFilters({});
      await loadDb(0, "", {});
      await loadStats();
    } catch (err) {
      setRunStatus(`Errore RUN: ${err.message}`);
    } finally {
      setRunLoading(false);
    }
  };

  const loadStats = async () => {
    try {
      const res = await fetch(`${API_BASE}/api/stats?t=${Date.now()}`, {
        cache: "no-store"
      });
      const data = await res.json();
      setStatsData({
        totalsByYear: data.totals_by_year || [],
        topSectors: data.top_sectors || [],
        topCities: data.top_cities || [],
        roundsByYear: data.rounds_by_year || [],
        checks: data.checks || {}
      });
    } catch {
      setStatsData({
        totalsByYear: [],
        topSectors: [],
        topCities: [],
        roundsByYear: [],
        checks: {}
      });
    }
  };

  useEffect(() => {
    loadStats();
  }, []);

  useEffect(() => {
    const onFocus = () => {
      loadStats();
    };
    window.addEventListener("focus", onFocus);
    return () => window.removeEventListener("focus", onFocus);
  }, []);

  const totalsByYear = statsData.totalsByYear || [];
  const topSectors = statsData.topSectors || [];
  const topCities = statsData.topCities || [];
  const roundsByYear = statsData.roundsByYear || [];

  const maxYearTotal = Math.max(1, ...totalsByYear.map((d) => d.total));
  const maxSectorTotal = Math.max(1, ...topSectors.map((d) => d.total));
  const maxCityTotal = Math.max(1, ...topCities.map((d) => d.total));
  const maxRoundsCount = Math.max(1, ...roundsByYear.map((d) => d.count));

  const pieColors = ["#0f8a4c", "#e53935", "#1e88e5", "#f9a825", "#7e57c2", "#26a69a"];
  const totalSectorAll = topSectors.reduce((acc, row) => acc + row.total, 0) || 1;
  const sectorPieSegments = topSectors.reduce((acc, row, idx) => {
    const pct = (row.total / totalSectorAll) * 100;
    const start = acc.total;
    const end = start + pct;
    acc.total = end;
    acc.segments.push({
      label: row.sector,
      total: row.total,
      color: pieColors[idx % pieColors.length],
      start,
      end
    });
    return acc;
  }, { total: 0, segments: [] }).segments;
  const sectorPieGradient = sectorPieSegments
    .map((s) => `${s.color} ${s.start}% ${s.end}%`)
    .join(", ");

  return (
    <div className="app">
      <header className="hero">
        <div>
          <span className="badge">{t.badge}</span>
          <h1>
            {t.heroTitle.split("\n")[0]}
            <br />
            {t.heroTitle.split("\n")[1]}
          </h1>
          <p>
            {t.heroBody.split("TWIS")[0]}
            <a className="link" href="https://dealflowit.niccolosanarico.com/" target="_blank" rel="noreferrer">
              TWIS
            </a>
            {t.heroBody.split("TWIS")[1]}
          </p>
        </div>
        <div className="hero-side">
          <div className="lang-toggle" role="group" aria-label="Language toggle">
            <button
              type="button"
              className={lang === "it" ? "lang active" : "lang"}
              onClick={() => setLang("it")}
            >
              🇮🇹 IT
            </button>
            <button
              type="button"
              className={lang === "en" ? "lang active" : "lang"}
              onClick={() => setLang("en")}
            >
              🇬🇧 EN
            </button>
          </div>
          <a
            className="logo-wrap"
            href="https://unitedventures.com/"
            target="_blank"
            rel="noreferrer"
          >
            <img className="uv-logo" src={uvLogo} alt="United Ventures" />
          </a>
        </div>
      </header>

      <section id="db-section" className="db">
        <div className="db-head">
          <div>
            <h2>{t.dbTitle}</h2>
            <p>{t.dbSubtitle}</p>
            {runStatus ? <p className="run-status">{runStatus}</p> : null}
          </div>
          <div className="db-actions">
            <button
              type="button"
              className="run-btn"
              onClick={handleRun}
              disabled={runLoading}
            >
              {runLoading ? t.running : t.run}
            </button>
            <form className="search" onSubmit={handleSearch}>
              <input
                type="text"
                placeholder={t.searchPlaceholder}
                value={dbSearch}
                onChange={(e) => setDbSearch(e.target.value)}
              />
              <button type="submit">{t.search}</button>
            </form>
          </div>
        </div>

        <div className="db-table">
          {dbLoading ? (
            <div className="muted">{t.loading}</div>
          ) : (
            <table>
              <thead>
                <tr>
                  {dbCols.map((c) => (
                    <th key={c}>{c}</th>
                  ))}
                </tr>
                <tr>
                  {dbCols.map((c) => (
                    <th key={`${c}-filter`}>
                      <select
                        className="filter"
                        value={dbFilters[c] || ""}
                        onChange={(e) => {
                          const next = { ...dbFilters, [c]: e.target.value };
                          setDbFilters(next);
                          setDbPage(0);
                          loadDb(0, "", next);
                        }}
                      >
                        <option value="">All {c}</option>
                        {c === "Round size (€M)"
                          ? roundSizeBuckets.map((b) => (
                              <option key={`${c}-${b.value}`} value={b.value}>{b.label}</option>
                            ))
                          : (filterOptions[c] || []).map((v) => (
                              <option key={`${c}-${v}`} value={v}>{v}</option>
                            ))}
                      </select>
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {dbRows.map((row, idx) => (
                  <tr key={idx}>
                    {dbCols.map((c) => (
                      <td key={c}>
                        {c === "Date" ? formatMonthYear(row[c]) : row[c]}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>

        <div className="pager">
          <span className="muted">{t.shown}: {dbRows.length}</span>
          <button
            className="ghost"
            onClick={() => {
              const next = Math.max(dbPage - 1, 0);
              setDbPage(next);
              loadDb(next, dbSearch, dbFilters);
            }}
            disabled={dbPage === 0}
          >
            {t.prev}
          </button>
          <span className="muted">Pagina {dbPage + 1}</span>
          <button
            className="ghost"
            onClick={() => {
              const next = dbPage + 1;
              setDbPage(next);
              loadDb(next, dbSearch, dbFilters);
            }}
          >
            {t.next}
          </button>
          <button
            className="ghost"
            onClick={() => {
              const nextExpanded = !dbExpanded;
              setDbExpanded(nextExpanded);
              setDbPage(0);
              loadDb(0, dbSearch, dbFilters, nextExpanded ? 50 : 10);
            }}
          >
            {dbExpanded ? t.reduce : t.expand}
          </button>
          <button
            className="ghost"
            onClick={() => {
              setDbFilters({});
              setDbSearch("");
              setDbPage(0);
              loadDb(0, "", {});
            }}
          >
            {t.clear}
          </button>
        </div>

        <div className="chat">
          <h3>{t.chatTitle}</h3>
          <div className="chat-form">
            <textarea
              rows={3}
              placeholder={t.chatPlaceholder}
              value={chatQ}
              onChange={(e) => setChatQ(e.target.value)}
            />
            <button
              type="button"
              onClick={() => {
                console.log("chat: click");
                handleChat();
              }}
              disabled={chatLoading}
            >
              {chatLoading ? t.analyzing : t.ask}
            </button>
          </div>
          <div className="chat-answer">
            {chatA ? chatA : t.chatEmpty}
          </div>
          {/* risposta solo testuale */}
        </div>
      </section>

      <section className="insights">
        <div className="insight-card">
          <div className="insight-head">
            <h3>{t.chartsYear}</h3>
            <span className="muted">{t.chartsYearSub}</span>
          </div>
          <div className="bars">
            {totalsByYear.map((row) => (
              <div className="bar-row" key={row.year}>
                <div className="bar-label">{row.year}</div>
                <div className="bar-track">
                  <div
                    className="bar-fill"
                    style={{ width: `${(row.total / maxYearTotal) * 100}%` }}
                  />
                </div>
                <div className="bar-value">{row.total.toFixed(2).replace(".", ",")}</div>
              </div>
            ))}
          </div>
        </div>

        <div className="insight-card">
          <div className="insight-head">
            <h3>{t.chartsSector}</h3>
            <span className="muted">{t.chartsSectorSub}</span>
          </div>
          <div className="pie-wrap">
            <div className="pie" style={{ background: `conic-gradient(${sectorPieGradient})` }} />
            <div className="pie-legend">
              {sectorPieSegments.map((seg) => (
                <div className="pie-item" key={seg.label}>
                  <span className="dot" style={{ background: seg.color }} />
                  <span>{seg.label}</span>
                  <strong>{seg.total.toFixed(2).replace(".", ",")}</strong>
                </div>
              ))}
            </div>
          </div>
        </div>

        <div className="insight-card">
          <div className="insight-head">
            <h3>{t.chartsCity}</h3>
            <span className="muted">{t.chartsCitySub}</span>
          </div>
          <div className="heatmap">
            {topCities.map((row) => {
              const intensity = row.total / maxCityTotal;
              return (
                <div
                  key={row.city}
                  className="heatmap-tile"
                  style={{
                    background: `rgba(15, 138, 76, ${0.15 + intensity * 0.75})`
                  }}
                >
                  <span>{row.city}</span>
                  <strong>{row.total.toFixed(2).replace(".", ",")}</strong>
                </div>
              );
            })}
          </div>
        </div>

        <div className="insight-card">
          <div className="insight-head">
            <h3>{t.chartsCount}</h3>
            <span className="muted">{t.chartsCountSub}</span>
          </div>
          <div className="line-chart">
            <svg viewBox="0 0 600 200" preserveAspectRatio="none">
              <polyline
                fill="none"
                stroke="#0f8a4c"
                strokeWidth="4"
                points={roundsByYear
                  .map((row, idx) => {
                    const x = (idx / (roundsByYear.length - 1)) * 560 + 20;
                    const y = 170 - (row.count / maxRoundsCount) * 130;
                    return `${x},${y}`;
                  })
                  .join(" ")}
              />
              {roundsByYear.map((row, idx) => {
                const x = (idx / (roundsByYear.length - 1)) * 560 + 20;
                const y = 170 - (row.count / maxRoundsCount) * 130;
                return <circle key={row.year} cx={x} cy={y} r="5" fill="#0f8a4c" />;
              })}
            </svg>
            <div className="line-labels">
              {roundsByYear.map((row) => (
                <div key={row.year} className="line-label">
                  <span className="line-year">{row.year}</span>
                  <strong className="line-count">{row.count}</strong>
                </div>
              ))}
            </div>
          </div>
        </div>
      </section>

      <footer className="footer">
        <div>{t.footer}</div>
      </footer>
    </div>
  );
}

function formatMonthYear(value) {
  if (!value) return "";
  const str = String(value);
  if (/\b\w{3}\s\d{4}\b/.test(str)) return str.match(/\b\w{3}\s\d{4}\b/)[0];
  if (/\b\w+\s\d{4}\b/.test(str)) return str.match(/\b\w+\s\d{4}\b/)[0];
  if (/\d{4}-\d{2}-\d{2}/.test(str)) {
    const [y, m] = str.split("-");
    const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    return `${months[parseInt(m, 10) - 1]} ${y}`;
  }
  return str;
}
