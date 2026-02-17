const runBtn = document.getElementById("runBtn");
const statusEl = document.getElementById("status");
const lastRunEl = document.getElementById("lastRun");
const lastRowsEl = document.getElementById("lastRows");
const lastUpdatedEl = document.getElementById("lastUpdated");
const companiesEl = document.getElementById("companies");
const logEl = document.getElementById("log");

let history = [];

function renderCompanies(list) {
  companiesEl.innerHTML = "";
  list.forEach((name) => {
    const chip = document.createElement("span");
    chip.className = "chip";
    chip.textContent = name;
    companiesEl.appendChild(chip);
  });
}

function renderLog() {
  logEl.innerHTML = "";
  history.forEach((run) => {
    const li = document.createElement("li");
    li.innerHTML = `<div><strong>${run.status}</strong> · ${run.time}</div><div class="muted">${run.rows} righe · ${run.companies.join(", ")}</div>`;
    logEl.appendChild(li);
  });
}

function updateSnapshot(run) {
  lastRunEl.textContent = run.time;
  lastRowsEl.textContent = `${run.rows} righe`;
  lastUpdatedEl.textContent = new Date().toLocaleString("it-IT");
  renderCompanies(run.companies);
}

async function runAutomation() {
  runBtn.disabled = true;
  statusEl.textContent = "Esecuzione in corso: parsing TWIS, aggiornamento Excel, backfill HQ...";
  try {
    const res = await fetch("/api/run", { method: "POST" });
    if (!res.ok) throw new Error("Run failed");
    const data = await res.json();
    const run = {
      time: data.time,
      status: data.status,
      rows: data.rows,
      companies: data.companies
    };
    history = [run, ...history].slice(0, 6);
    updateSnapshot(run);
    renderLog();
    statusEl.textContent = "Run completato: Excel aggiornato.";
  } catch (err) {
    statusEl.textContent = "Errore: impossibile eseguire l’automazione. Controlla il backend.";
  } finally {
    runBtn.disabled = false;
  }
}

runBtn.addEventListener("click", runAutomation);

const initial = {
  time: new Date().toLocaleString("it-IT", { day: "2-digit", month: "short", year: "numeric", hour: "2-digit", minute: "2-digit" }),
  status: "Idle",
  rows: "—",
  companies: []
};
updateSnapshot(initial);
renderLog();
