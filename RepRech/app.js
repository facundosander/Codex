const storageKey = "repRechFilters";

const state = {
  rows: [],
  total: 0,
  countAll: 0,
  resolvedCount: 0,
  bounds: null,
  filters: {
    from: null,
    to: null,
    detail: ""
  },
  page: 1,
  pageSize: 20,
  totalPages: 1
};

const ui = {};

window.addEventListener("DOMContentLoaded", () => {
  cacheElements();
  loadFiltersFromStorage();
  attachListeners();
  loadRows().catch((error) => {
    handleError(error, "No se pudieron obtener los datos iniciales.");
  });
});

function cacheElements() {
  ui.fileInput = document.getElementById("fileInput");
  ui.removeResolved = document.getElementById("removeResolved");
  ui.fromDate = document.getElementById("fromDate");
  ui.toDate = document.getElementById("toDate");
  ui.detailFilter = document.getElementById("detailFilter");
  ui.clearFilters = document.getElementById("clearFilters");
  ui.summary = document.getElementById("summary");
  ui.tbody = document.querySelector("#dataTable tbody");
  ui.pagination = document.getElementById("paginationControls");
  ui.prevPage = document.getElementById("prevPage");
  ui.nextPage = document.getElementById("nextPage");
  ui.pageInfo = document.getElementById("pageInfo");
}

function attachListeners() {
  if (ui.fileInput) {
    ui.fileInput.addEventListener("change", async (event) => {
      try {
        await handleFileUpload(event);
      } catch (error) {
        handleError(error, "No se pudo procesar el archivo seleccionado.");
      }
    });
  }

  if (ui.removeResolved) {
    ui.removeResolved.addEventListener("click", async () => {
      try {
        state.page = 1;
        await removeResolvedRows();
      } catch (error) {
        handleError(error, "No se pudieron eliminar las filas resueltas.");
      }
    });
  }

  if (ui.fromDate) {
    ui.fromDate.addEventListener("change", () => {
      state.filters.from = ui.fromDate.value || null;
      ensureValidDateRange();
      state.page = 1;
      saveFilters();
      loadRows().catch((error) => handleError(error, "No se pudieron aplicar los filtros."));
    });
  }

  if (ui.toDate) {
    ui.toDate.addEventListener("change", () => {
      state.filters.to = ui.toDate.value || null;
      ensureValidDateRange();
      state.page = 1;
      saveFilters();
      loadRows().catch((error) => handleError(error, "No se pudieron aplicar los filtros."));
    });
  }

  if (ui.detailFilter) {
    ui.detailFilter.addEventListener("input", () => {
      state.filters.detail = ui.detailFilter.value || "";
      state.page = 1;
      saveFilters();
      loadRows().catch((error) => handleError(error, "No se pudieron aplicar los filtros."));
    });
  }

  if (ui.clearFilters) {
    ui.clearFilters.addEventListener("click", () => {
      state.filters = { from: null, to: null, detail: "" };
      state.page = 1;
      saveFilters();
      loadRows().catch((error) => handleError(error, "No se pudieron restablecer los filtros."));
    });
  }

  if (ui.prevPage) {
    ui.prevPage.addEventListener("click", () => {
      goToPage(state.page - 1);
    });
  }

  if (ui.nextPage) {
    ui.nextPage.addEventListener("click", () => {
      goToPage(state.page + 1);
    });
  }
}

function goToPage(targetPage) {
  if (targetPage < 1 || targetPage > state.totalPages) {
    return;
  }
  state.page = targetPage;
  loadRows().catch((error) => handleError(error, "No se pudo cambiar de p?gina."));
}

function loadFiltersFromStorage() {
  try {
    const stored = localStorage.getItem(storageKey);
    if (!stored) {
      return;
    }
    const parsed = JSON.parse(stored);
    state.filters.from = parsed.from || null;
    state.filters.to = parsed.to || null;
    state.filters.detail = parsed.detail || "";
  } catch (error) {
    console.warn("No se pudieron restaurar los filtros almacenados", error);
  }
}

function saveFilters() {
  try {
    localStorage.setItem(
      storageKey,
      JSON.stringify({
        from: state.filters.from,
        to: state.filters.to,
        detail: state.filters.detail
      })
    );
  } catch (error) {
    console.warn("No se pudieron guardar los filtros", error);
  }
}

async function loadRows(options = {}) {
  const { additionalMessage, skipDefaulting = false } = options;

  const params = new URLSearchParams();
  params.append("page", String(state.page));
  params.append("page_size", String(state.pageSize));

  if (state.filters.from) {
    params.append("from", state.filters.from);
  }
  if (state.filters.to) {
    params.append("to", state.filters.to);
  }
  if (state.filters.detail) {
    params.append("detail", state.filters.detail);
  }

  const url = `/api/rows?${params.toString()}`;

  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }

  const payload = await response.json();

  state.rows = Array.isArray(payload.rows) ? payload.rows : [];
  state.total = typeof payload.total === "number" ? payload.total : state.rows.length;
  state.countAll = typeof payload.count_all === "number" ? payload.count_all : state.total;
  state.resolvedCount = typeof payload.resolved_count === "number" ? payload.resolved_count : 0;
  state.bounds = payload.bounds || state.bounds;
  state.page = typeof payload.page === "number" ? payload.page : state.page;
  state.pageSize = typeof payload.page_size === "number" ? payload.page_size : state.pageSize;
  state.totalPages = typeof payload.total_pages === "number"
    ? Math.max(1, payload.total_pages)
    : Math.max(1, Math.ceil((state.total || 0) / state.pageSize));

  if (
    !skipDefaulting &&
    (!state.filters.from || !state.filters.to) &&
    state.bounds &&
    state.bounds.min &&
    state.bounds.max &&
    applyDefaultFilters()
  ) {
    saveFilters();
    state.page = 1;
    await loadRows({ additionalMessage, skipDefaulting: true });
    return;
  }

  syncFilterInputs();
  updateFilterBounds();
  renderTable();
  updateRemoveResolvedButton();
  renderPagination();
  updateSummary(additionalMessage ? { text: additionalMessage } : null);
}

function applyDefaultFilters() {
  if (!state.bounds || !state.bounds.min || !state.bounds.max) {
    return false;
  }

  const minDate = new Date(state.bounds.min);
  const maxDate = new Date(state.bounds.max);
  if (Number.isNaN(minDate.getTime()) || Number.isNaN(maxDate.getTime())) {
    return false;
  }

  const from = `${minDate.getFullYear()}-01-01`;
  const to = `${maxDate.getFullYear()}-12-31`;
  const changed = state.filters.from !== from || state.filters.to !== to || state.filters.detail;

  state.filters.from = from;
  state.filters.to = to;
  state.filters.detail = "";

  return changed;
}

function syncFilterInputs() {
  if (ui.fromDate) {
    ui.fromDate.value = state.filters.from || "";
  }
  if (ui.toDate) {
    ui.toDate.value = state.filters.to || "";
  }
  if (ui.detailFilter) {
    ui.detailFilter.value = state.filters.detail || "";
  }
}

function updateFilterBounds() {
  if (!ui.fromDate || !ui.toDate) {
    return;
  }

  if (!state.bounds || !state.bounds.min || !state.bounds.max) {
    ui.fromDate.min = "";
    ui.fromDate.max = "";
    ui.toDate.min = "";
    ui.toDate.max = "";
    ui.fromDate.disabled = state.countAll === 0;
    ui.toDate.disabled = state.countAll === 0;
    return;
  }

  ui.fromDate.disabled = false;
  ui.toDate.disabled = false;

  ui.fromDate.min = state.bounds.min;
  ui.fromDate.max = state.bounds.max;
  ui.toDate.max = state.bounds.max;
  ui.toDate.min = state.filters.from || state.bounds.min;
}

function ensureValidDateRange() {
  if (!state.filters.from || !state.filters.to) {
    return;
  }
  if (state.filters.from > state.filters.to) {
    state.filters.to = state.filters.from;
    if (ui.toDate) {
      ui.toDate.value = state.filters.to;
    }
  }
}

function renderTable() {
  if (!ui.tbody) {
    return;
  }

  const fragment = document.createDocumentFragment();

  if (!state.rows.length) {
    const row = document.createElement("tr");
    const cell = document.createElement("td");
    cell.colSpan = 7;
    cell.className = "table-placeholder";
    cell.textContent = state.countAll
      ? "No hay filas que coincidan con los filtros activos."
      : "No hay datos cargados. Usa el bot?n para importar un RPT.";
    row.appendChild(cell);
    fragment.appendChild(row);
  } else {
    for (const rowData of state.rows) {
      const tr = document.createElement("tr");
      tr.dataset.key = rowData.id;
      if (rowData.resolved) {
        tr.classList.add("resolved");
      }

      appendCell(tr, rowData.EmpRUC);
      appendCell(tr, rowData.EmpRazonSocial);
      appendCell(tr, rowData.EmpNom);
      appendCell(tr, rowData.RepFecha);
      appendCell(tr, rowData.RepLiqEstadoConsulta);
      appendCell(tr, rowData.RepDetalleRechazo);

      const actionsCell = document.createElement("td");
      const toggleButton = document.createElement("button");
      toggleButton.type = "button";
      toggleButton.className = "action-btn";
      toggleButton.textContent = rowData.resolved ? "Marcar pendiente" : "Marcar resuelto";
      toggleButton.addEventListener("click", () => {
        toggleResolved(rowData.id).catch((error) => {
          handleError(error, "No se pudo actualizar el estado de la fila.");
        });
      });
      actionsCell.appendChild(toggleButton);
      tr.appendChild(actionsCell);

      fragment.appendChild(tr);
    }
  }

  ui.tbody.replaceChildren(fragment);
}

function appendCell(row, value) {
  const cell = document.createElement("td");
  cell.textContent = value ?? "";
  row.appendChild(cell);
}

function renderPagination() {
  if (!ui.pagination || !ui.pageInfo || !ui.prevPage || !ui.nextPage) {
    return;
  }

  if (state.totalPages <= 1) {
    ui.pagination.style.display = "none";
    return;
  }

  ui.pagination.style.display = "flex";
  ui.pageInfo.textContent = `P?gina ${state.page} de ${state.totalPages}`;
  ui.prevPage.disabled = state.page <= 1;
  ui.nextPage.disabled = state.page >= state.totalPages;
}

async function handleFileUpload(event) {
  const files = Array.from(event.target.files || []);
  if (!files.length) {
    return;
  }

  const formData = new FormData();
  files.forEach((file) => formData.append("files", file));

  const response = await fetch("/api/upload", {
    method: "POST",
    body: formData
  });

  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }

  const payload = await response.json();
  const message = buildUploadMessage(payload);

  event.target.value = "";
  state.page = 1;

  await loadRows({ additionalMessage: message });
}

function buildUploadMessage(payload) {
  if (!payload || !Array.isArray(payload.files) || !payload.files.length) {
    return "Importaci?n completada.";
  }

  const parts = payload.files.map((file) => {
    const name = file.name || "archivo";
    const total = typeof file.total === "number" ? file.total : 0;
    const added = typeof file.added === "number" ? file.added : 0;
    const duplicates = typeof file.duplicates === "number" ? file.duplicates : 0;
    const ignored = typeof file.ignored === "number" ? file.ignored : 0;
    return `${name}: ${added} nuevas de ${total}. Duplicadas: ${duplicates}. Ignoradas: ${ignored}.`;
  });

  return parts.join(" ");
}

async function toggleResolved(id) {
  const response = await fetch(`/api/rows/${encodeURIComponent(id)}/toggle`, {
    method: "POST"
  });
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  await loadRows();
}

async function removeResolvedRows() {
  const response = await fetch("/api/rows/resolved", {
    method: "DELETE"
  });
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  await loadRows({ additionalMessage: "Filas resueltas eliminadas." });
}

function updateRemoveResolvedButton() {
  if (!ui.removeResolved) {
    return;
  }
  ui.removeResolved.disabled = state.resolvedCount === 0;
}

function updateSummary(info) {
  if (!ui.summary) {
    return;
  }

  const baseText = `Mostrando ${state.total}/${state.countAll}. Resueltas: ${state.resolvedCount}. P?gina ${state.page}/${state.totalPages}.`;

  if (info?.error) {
    ui.summary.textContent = `${info.error} ${baseText}`;
    ui.summary.classList.add("summary--error");
    return;
  }

  ui.summary.classList.remove("summary--error");

  if (info?.text) {
    ui.summary.textContent = `${info.text} ${baseText}`;
  } else {
    ui.summary.textContent = baseText;
  }
}

function handleError(error, message) {
  console.error(message, error);
  updateSummary({ error: message });
}
