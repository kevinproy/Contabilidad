document.addEventListener('DOMContentLoaded', () => {
  // Debug helpers (puedes desactivar logs en runtime: window.DEBUG_LOG=false)
  window.DEBUG_LOG = true;
  const log = (...args) => { if (window.DEBUG_LOG) console.log('[EC]', ...args); };
  const warn = (...args) => { if (window.DEBUG_LOG) console.warn('[EC]', ...args); };
  const err = (...args) => { console.error('[EC]', ...args); };
  log('DOMContentLoaded');
  window.addEventListener('error', (e) => err('window.error', e.message, e.error));
  window.addEventListener('unhandledrejection', (e) => err('unhandledrejection', e.reason));
  const toggles = document.querySelectorAll('.nav-group-toggle');
  toggles.forEach((btn) => {
    btn.addEventListener('click', () => {
      const targetId = btn.getAttribute('data-target');
      const sub = document.getElementById(targetId);
      if (!sub) return;
      const parent = btn.closest('.nav-group');
      const isOpen = parent && parent.classList.contains('open');
      if (isOpen) {
        sub.style.display = 'none';
        parent.classList.remove('open');
      } else {
        sub.style.display = 'block';
        parent.classList.add('open');
      }
    });
  });

  // Reordenar por arrastre y guardar orden
  const tables = document.querySelectorAll('table.reorder-table');
  tables.forEach((table) => {
    const tbody = table.querySelector('tbody');
    if (!tbody) return;
    let dragRow = null;
    const isValidId = (v) => typeof v === 'string' && /^\d+$/.test(v.trim());
    tbody.querySelectorAll('tr').forEach((tr) => {
      const id = tr.getAttribute('data-id');
      if (isValidId(id)) {
        tr.setAttribute('draggable', 'true');
      } else {
        tr.removeAttribute('draggable');
      }
      tr.addEventListener('dragstart', (e) => {
        if (!isValidId(id)) { e.preventDefault(); return; }
        dragRow = tr;
        tr.classList.add('dragging');
        try {
          if (e.dataTransfer) {
            e.dataTransfer.effectAllowed = 'move';
            e.dataTransfer.setData('text/plain', id);
          }
        } catch (_) {}
        log('dragstart', { id });
      });
      tr.addEventListener('dragend', () => {
        tr.classList.remove('dragging');
        dragRow = null;
        const cliente = table.getAttribute('data-cliente');
        const ids = Array.from(tbody.querySelectorAll('tr'))
          .map(r => r.getAttribute('data-id'))
          .filter(isValidId);
        if (!cliente || ids.length === 0) return;
        log('reordenar POST', { cliente, idsCount: ids.length });
        fetch(`/estado-cuenta/reordenar/${encodeURIComponent(cliente)}`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ ids }),
        })
          .then((r) => { log('reordenar resp', r.status); })
          .then(() => window.location.reload())
          .catch((e) => { err('reordenar error', e); });
      });
    });
    tbody.addEventListener('dragover', (e) => {
      e.preventDefault();
      try { if (e.dataTransfer) e.dataTransfer.dropEffect = 'move'; } catch (_) {}
      const after = getDragAfterElement(tbody, e.clientY);
      if (!dragRow) return;
      if (after == null) {
        tbody.appendChild(dragRow);
      } else {
        if (after.parentNode === tbody) tbody.insertBefore(dragRow, after);
      }
    });
    function getDragAfterElement(container, y) {
      const els = [...container.querySelectorAll('tr:not(.dragging)')];
      return els.reduce((closest, child) => {
        const box = child.getBoundingClientRect();
        const offset = y - box.top - box.height / 2;
        if (offset < 0 && offset > closest.offset) {
          return { offset, element: child };
        } else {
          return closest;
        }
      }, { offset: Number.NEGATIVE_INFINITY, element: null }).element;
    }
  });

  // Marcado de celdas numéricas y sumatoria al vuelo
  document.querySelectorAll('td.num-cell').forEach((td) => {
    // Limpieza preventiva de clases incorrectas
    if (!td.classList.contains('mark-1') && !td.classList.contains('mark-2') && !td.classList.contains('mark-3') && !td.classList.contains('mark-4') && !td.classList.contains('mark-5') && !td.classList.contains('mark-6') && !td.classList.contains('mark-7') && !td.classList.contains('mark-8') && !td.classList.contains('mark-9')) {
      td.classList.remove('marked');
    }
    td.addEventListener('click', (e) => {
      const id = td.getAttribute('data-id');
      const col = td.getAttribute('data-col');
      let mark = -1;
      if (e.shiftKey) mark = 0; else if (e.ctrlKey || e.metaKey) mark = 1;
      log('mark click', { id, col, mark });
      fetch(`/estado-cuenta/marcar/${encodeURIComponent(id)}/${encodeURIComponent(col)}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ mark }),
      })
        .then(r => r.json())
        .then(data => {
          log('mark resp', data);
          if (!data || data.ok === false) return;
          // actualizar clases visualmente sin recargar
          td.classList.remove('marked');
          for (let i = 1; i <= 9; i++) td.classList.remove(`mark-${i}`);
          const m = parseInt(data.mark || 0);
          if (m > 0) { td.classList.add('marked', `mark-${m}`); }
          computeSumBar(td.closest('table'));
        })
        .catch((e) => { err('mark error', e); });
    });
    td.addEventListener('mouseenter', () => updateHoverSum(td));
    td.addEventListener('mousemove', () => updateHoverSum(td));
  });

  // Botón de pagado (usa doble click en la celda DIAS o VENCIMIENTO)
  document.querySelectorAll('td.venc-cell, td.venc-days').forEach((td) => {
    td.addEventListener('dblclick', () => {
      const tr = td.closest('tr');
      const id = tr && tr.getAttribute('data-id');
      if (!id) return;
      fetch(`/estado-cuenta/pagado/${encodeURIComponent(id)}`, { method: 'POST' })
        .then(r => r.json())
        .then((data) => {
          if (!data || data.ok === false) return;
          // quitar highlight en la fila
          tr.querySelectorAll('.overdue').forEach((el) => el.classList.remove('overdue'));
        })
        .catch((e) => err('pagado error', e));
    });
  });

  // Capturar número después de click: click + 1..9
  let lastClickedCell = null;
  document.addEventListener('click', (e) => {
    const cell = e.target.closest('td.num-cell');
    if (cell) lastClickedCell = cell;
  });
  document.addEventListener('keydown', (e) => {
    if (!lastClickedCell) return;
    const d = parseInt(e.key);
    if (isNaN(d) || d < 0 || d > 9) return;
    const id = lastClickedCell.getAttribute('data-id');
    const col = lastClickedCell.getAttribute('data-col');
    log('mark keydown', { id, col, d });
    fetch(`/estado-cuenta/marcar/${encodeURIComponent(id)}/${encodeURIComponent(col)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ mark: d }),
    }).then(r => r.json()).then(data => {
      log('mark resp', data);
      if (!data || data.ok === false) return;
      lastClickedCell.classList.remove('marked');
      for (let i = 1; i <= 9; i++) lastClickedCell.classList.remove(`mark-${i}`);
      const m = parseInt(data.mark || 0);
      if (m > 0) { lastClickedCell.classList.add('marked', `mark-${m}`); }
      computeSumBar(lastClickedCell.closest('table'));
    }).catch((e) => { err('mark error', e); });
  });

  function updateHoverSum(td) {
    const table = td.closest('table');
    const col = td.getAttribute('data-col');
    if (!table || !col) return;
    const marked = table.querySelectorAll(`td.num-cell.marked[data-col='${col}']`);
    let sum = 0;
    marked.forEach((cell) => {
      const raw = cell.getAttribute('data-val') ?? '0';
      const n = parseNum(raw);
      sum += n;
    });
    let tip = td.querySelector('.hover-sum');
    if (!tip) {
      tip = document.createElement('span');
      tip.className = 'hover-sum';
      td.appendChild(tip);
    }
    tip.textContent = new Intl.NumberFormat('es-ES', { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(sum);
  }

  // Barra de sumas por cliente (solo Debe/Haber)
  function computeSumBar(table) {
    if (!table) return;
    const card = table.closest('.card');
    if (!card) return;
    let bar = card.querySelector('.sum-bar');
    const sumDebe = sumMarked(table, 'DEBE');
    const sumHaber = sumMarked(table, 'HABER');
    if (!bar) {
      bar = document.createElement('div');
      bar.className = 'sum-bar';
      const parent = table.parentNode || card;
      // Insert before the table within its direct parent to avoid NotFoundError
      parent.insertBefore(bar, table);
    }
    bar.innerHTML = `Σ Debe: <strong>${fmt(sumDebe)}</strong> · Σ Haber: <strong>${fmt(sumHaber)}</strong>`;
    bar.style.display = (sumDebe === 0 && sumHaber === 0) ? 'none' : 'block';
  }

  function sumMarked(table, col) {
    let s = 0;
    table.querySelectorAll(`td.num-cell.marked[data-col='${col}']`).forEach((cell) => {
      const raw = cell.getAttribute('data-val') ?? '0';
      s += parseNum(raw);
    });
    return s;
  }

  function fmt(n) {
    return new Intl.NumberFormat('es-ES', { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(n);
  }

  // Inicializar barras en todas las tablas
  document.querySelectorAll('table.reorder-table').forEach((t) => computeSumBar(t));

  // Ordenar por fecha al hacer click en el encabezado FECHA
  document.querySelectorAll('th.sortable[data-sort="fecha"]').forEach((th) => {
    th.style.cursor = 'pointer';
    th.addEventListener('click', () => {
      const table = th.closest('table');
      if (!table) return;
      const current = (table.getAttribute('data-orden') || 'asc').toLowerCase();
      const next = current === 'asc' ? 'desc' : 'asc';
      const url = new URL(window.location.href);
      url.searchParams.set('orden', next);
      window.location.href = url.toString();
    });
  });

  function parseNum(raw) {
    // robusto: intenta directo (US/JSON), luego convierte formato ES
    const str = String(raw).trim();
    let n = Number(str);
    if (!Number.isNaN(n)) return n;
    const es = str.replace(/\./g, '').replace(',', '.');
    n = Number(es);
    return Number.isNaN(n) ? 0 : n;
  }
});


