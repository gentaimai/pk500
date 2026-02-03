async function loadHistory() {
  const res = await fetch("index_history.csv", { cache: "no-store" });
  if (!res.ok) {
    throw new Error("index_history.csv not found");
  }
  const text = await res.text();
  const lines = text.trim().split(/\r?\n/);
  if (lines.length <= 1) return [];

  const headers = lines[0].split(",");
  const idx = (name) => headers.indexOf(name);
  const tsIdx = idx("run_timestamp_iso");
  const tsLocalIdx = idx("run_timestamp_local");
  const totalIdx = idx("total_cards");
  const basketIdx = idx("basket_size");
  const sumIdx = idx("basket_sum_value_usd");
  const popIdx = idx("basket_total_pop10");
  const indexIdx = idx("pk500_avg_usd");

  return lines.slice(1).map((line) => {
    const cols = line.split(",");
    const indexVal = cols[indexIdx] ? Number(cols[indexIdx]) : null;
    return {
      iso: cols[tsIdx],
      local: cols[tsLocalIdx],
      total: Number(cols[totalIdx] || 0),
      basket: Number(cols[basketIdx] || 0),
      sum: Number(cols[sumIdx] || 0),
      pop10: Number(cols[popIdx] || 0),
      index: Number.isFinite(indexVal) ? indexVal : null,
    };
  }).filter((d) => d.iso);
}

function renderSummary(latest) {
  const fmt = (n) => Number(n).toLocaleString(undefined, { maximumFractionDigits: 2 });

  document.getElementById("latest-index").textContent =
    latest.index !== null ? fmt(latest.index) : "N/A";
  document.getElementById("last-updated").textContent = `Last updated: ${latest.local}`;
  document.getElementById("basket-size").textContent = fmt(latest.basket);
  document.getElementById("basket-sum").textContent = `Basket sum: $${fmt(latest.sum)}`;
  document.getElementById("universe-count").textContent = fmt(latest.total);
}

function renderChart(history) {
  const labels = history.map((d) => d.iso.split("T")[0]);
  const data = history.map((d) => d.index);

  const ctx = document.getElementById("indexChart").getContext("2d");
  // eslint-disable-next-line no-new
  new Chart(ctx, {
    type: "line",
    data: {
      labels,
      datasets: [
        {
          label: "PK500-A",
          data,
          tension: 0.2,
          borderColor: "#7af2c2",
          backgroundColor: "rgba(122, 242, 194, 0.2)",
          pointRadius: 2.8,
        },
      ],
    },
    options: {
      responsive: true,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: (ctx) => `PK500-A: ${ctx.formattedValue}`,
          },
        },
      },
      scales: {
        x: {
          ticks: { color: "#dce4ff" },
          grid: { color: "rgba(255,255,255,0.05)" },
        },
        y: {
          ticks: { color: "#dce4ff" },
          grid: { color: "rgba(255,255,255,0.05)" },
        },
      },
    },
  });
}

function renderTable(history) {
  const recent = history.slice(-10).reverse(); // latest first
  const container = document.getElementById("table-container");
  if (!recent.length) {
    container.innerHTML = "<p class=\"meta\">No data yet.</p>";
    return;
  }

  const rows = recent
    .map(
      (d) =>
        `<tr>
          <td>${d.local}</td>
          <td>${d.index !== null ? d.index.toFixed(2) : "—"}</td>
          <td>${d.basket}</td>
          <td>${d.total}</td>
        </tr>`
    )
    .join("");

  container.innerHTML = `
    <table>
      <thead>
        <tr>
          <th>Date (local)</th>
          <th>PK500-A</th>
          <th>Basket size</th>
          <th>Universe</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
  `;
}

async function main() {
  try {
    const history = await loadHistory();
    if (!history.length) {
      document.getElementById("summary").innerHTML =
        "<p class=\"meta\">index_history.csv has no data yet. Run the workflow to generate the first point.</p>";
      return;
    }
    const latest = history[history.length - 1];
    renderSummary(latest);
    renderChart(history.filter((d) => d.index !== null));
    renderTable(history);
  } catch (err) {
    document.getElementById("summary").innerHTML =
      `<p class="meta">Could not load index history: ${err.message}</p>`;
  }
}

main();
