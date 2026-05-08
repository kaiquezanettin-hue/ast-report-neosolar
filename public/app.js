/* ═══════════════════════════════════════════════
   AST Report — NeoSolar  |  app.js
   ═══════════════════════════════════════════════ */

// ── Estado global ────────────────────────────────────────────────────
let state = { report: null, files: {} };
let charts = {};
let prodSelecionado = null;
let prodGarantiaFiltro = 'todos';
let deskTickets = [], deskLoaded = false;
let deskHistory = {}, historyLoading = false, historyLoaded = false;

// ── Paleta e constantes de cor ───────────────────────────────────────
const C = {
  yellow: '#FFD04C', white: '#FFFDF0', gray: '#565759', gray2: '#888',
  red: '#e05252', green: '#52c878', orange: '#e09052',
  blue: '#5299e0', purple: '#9b59e0',
  bg2: '#242628', bg3: '#2c2e30', border: '#353739'
};

// ── Domínio: mapeamento de status ────────────────────────────────────
const STATUS_GRUPOS = {
  'Aguardando 1º contato - AST': 'ENTRADA', 'Pendencia AST - SAC': 'ENTRADA',
  'Aguardando teste': 'AVALIAÇÃO', 'Em teste': 'AVALIAÇÃO', 'Ag. teste': 'AVALIAÇÃO',
  'Aguardando manutençao (SG)': 'TÉCNICO', 'Em manutençao': 'TÉCNICO', 'Em manutenção': 'TÉCNICO',
  'Aguardando laudo': 'TÉCNICO', 'Aguardando RMA': 'TÉCNICO',
  'Ag. Peça Reposição': 'TÉCNICO', 'Aguardando Peça Reposição': 'TÉCNICO',
  'Aguardando aprovaçao de manutençao (SG)': 'PAGAMENTO', 'Ag. aprovação manutenção (SG)': 'PAGAMENTO',
  'Em tratativa p/ devoluçao cliente': 'DEVOLUÇÃO', 'Em tratativa p/ devolução cliente': 'DEVOLUÇÃO',
  'Aguardando coleta': 'DEVOLUÇÃO',
  'Ag. Prazo / Autorização Descarte': 'DEVOLUÇÃO', 'Aguardando Prazo / Autorização Descarte': 'DEVOLUÇÃO'
};

const SLA_DIAS_UTEIS = {
  'Pendencia AST - SAC': 2.0, 'Aguardando 1º contato - AST': 0.5,
  'Aguardando teste': 3.0, 'Em teste': 0.25,
  'Em manutençao': 0.25, 'Em manutenção': 0.25,
  'Aguardando aprovaçao de manutençao (SG)': 2.0, 'Ag. aprovação manutenção (SG)': 2.0,
  'Aguardando manutençao (SG)': 1.0, 'Aguardando laudo': 0.13,
  'Aguardando RMA': 0.5, 'Ag. Peça Reposição': 0.5, 'Aguardando Peça Reposição': 0.5,
  'Em tratativa p/ devoluçao cliente': 2.0, 'Aguardando coleta': 1.0
};

// Status que geram custo de atendimento técnico
const STATUS_CUSTO = new Set(['Em teste', 'Em manutençao', 'Em manutenção', 'Aguardando laudo', 'Aguardando RMA']);
const CUSTO_POR_HORA = 120; // R$2/min = R$120/hora

// Status ignorados em cálculos
const STATUS_CONCLUIDOS = new Set(['Descarte', 'Produto despachado AST', 'Produto despachado']);

// ── Relógio ──────────────────────────────────────────────────────────
setInterval(() => {
  document.getElementById('clock').textContent = new Date().toLocaleTimeString('pt-BR');
}, 1000);

// ── Período ──────────────────────────────────────────────────────────
function setPeriod(days) {
  const to = new Date(), from = new Date();
  from.setDate(from.getDate() - days);
  document.getElementById('date-from').value = from.toISOString().slice(0, 10);
  document.getElementById('date-to').value = to.toISOString().slice(0, 10);
  document.querySelectorAll('.period-shortcuts button').forEach(b => b.classList.remove('active'));
  const id = { 7: 'sh-7', 30: 'sh-30', 90: 'sh-90', 365: 'sh-365' }[days];
  if (id) document.getElementById(id).classList.add('active');
}
function setPeriodAll() {
  document.getElementById('date-from').value = '2022-01-01';
  document.getElementById('date-to').value = new Date().toISOString().slice(0, 10);
  document.querySelectorAll('.period-shortcuts button').forEach(b => b.classList.remove('active'));
  document.getElementById('sh-all').classList.add('active');
}

// ── Navegação de abas ────────────────────────────────────────────────
function switchTab(id) {
  document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.getElementById(`tab-${id}`).classList.add('active');
  event.target.classList.add('active');
  if (id === 'produtos' && window._lastReportData) setTimeout(() => renderProdutos(window._lastReportData), 80);
  if (id === 'sla' && typeof renderSlaReport === 'function') setTimeout(() => renderSlaReport(), 80);
  if (id === 'operacao' && window._lastReportData) setTimeout(() => renderOperacao(window._lastReportData), 80);
}

// ── Chart.js — configuração global ──────────────────────────────────
Chart.register({
  id: 'centerText',
  beforeDraw(chart) {
    if (chart.config.options?.centerText) {
      const { width, height, ctx } = chart; ctx.save();
      ctx.textAlign = 'center'; ctx.textBaseline = 'middle';
      const cx = width / 2, cy = height / 2;
      ctx.font = 'bold 32px Barlow Condensed, sans-serif'; ctx.fillStyle = '#FFFDF0';
      ctx.fillText(chart.config.options.centerText.line1, cx, cy - 10);
      ctx.font = '11px Barlow, sans-serif'; ctx.fillStyle = '#888888';
      ctx.fillText(chart.config.options.centerText.line2, cx, cy + 16);
      ctx.restore();
    }
  }
});
Chart.defaults.color = C.gray2;
Chart.defaults.borderColor = C.border;
Chart.defaults.font.family = 'Barlow, sans-serif';

function makeChart(id, config) {
  if (charts[id]) charts[id].destroy();
  const ctx = document.getElementById(id);
  if (!ctx) return;
  charts[id] = new Chart(ctx, config);
  return charts[id];
}
function palette(n) {
  const cols = [C.yellow, C.blue, C.green, C.orange, C.purple, C.red, '#52d4e0', '#e052a0', '#a0e052', '#52a0e0'];
  return Array.from({ length: n }, (_, i) => cols[i % cols.length]);
}

// ── Horas úteis e feriados ───────────────────────────────────────────
function calcPascoa(ano) {
  const a=ano%19,b=Math.floor(ano/100),c=ano%100,d=Math.floor(b/4),e=b%4,f=Math.floor((b+8)/25),g=Math.floor((b-f+1)/3),h=(19*a+b-d-g+15)%30,i=Math.floor(c/4),k=c%4,l=(32+2*e+2*i-h-k)%7,m=Math.floor((a+11*h+22*l)/451),mes=Math.floor((h+l-7*m+114)/31),dia=((h+l-7*m+114)%31)+1;
  return new Date(ano, mes-1, dia);
}
function getFeriadosBR(ano) {
  const p=calcPascoa(ano),add=(d,n)=>{const r=new Date(d);r.setDate(r.getDate()+n);return r;},fmt=d=>`${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
  return new Set([`${ano}-01-01`,`${ano}-04-21`,`${ano}-05-01`,`${ano}-09-07`,`${ano}-10-12`,`${ano}-11-02`,`${ano}-11-15`,`${ano}-12-25`,fmt(add(p,-48)),fmt(add(p,-47)),fmt(add(p,-2)),fmt(p),fmt(add(p,60))]);
}
const _feriadosCache = {};
function getFeriados(ano) { if (!_feriadosCache[ano]) _feriadosCache[ano] = getFeriadosBR(ano); return _feriadosCache[ano]; }

const HORA_INICIO = 9, HORA_FIM = 18, HORAS_DIA_UTIL = 9;

function isDiaUtil(date) {
  const dow = date.getDay(); if (dow === 0 || dow === 6) return false;
  const s = `${date.getFullYear()}-${String(date.getMonth()+1).padStart(2,'0')}-${String(date.getDate()).padStart(2,'0')}`;
  return !getFeriados(date.getFullYear()).has(s);
}
function horasUteisEntre(inicio, fim) {
  if (!inicio || !fim || fim <= inicio) return 0;
  let total = 0; const cur = new Date(inicio);
  while (cur < fim) {
    const dia = new Date(cur); dia.setHours(0,0,0,0);
    if (!isDiaUtil(cur)) { cur.setDate(cur.getDate()+1); cur.setHours(0,0,0,0); continue; }
    const ei = new Date(dia); ei.setHours(HORA_INICIO,0,0,0);
    const ef = new Date(dia); ef.setHours(HORA_FIM,0,0,0);
    const si = Math.max(cur.getTime(), ei.getTime()), sf = Math.min(fim.getTime(), ef.getTime());
    if (sf > si) total += (sf-si) / 3600000;
    cur.setDate(cur.getDate()+1); cur.setHours(0,0,0,0);
  }
  return total;
}
function horasParaDiasUteis(horas, startTime) {
  if (!startTime || horas <= 0) return 0;
  const inicio = new Date(startTime), fim = new Date(inicio.getTime() + horas * 3600000);
  return horasUteisEntre(inicio, fim) / HORAS_DIA_UTIL;
}

// ── Parsers de data ──────────────────────────────────────────────────
function parseDataBR(str) {
  if (!str) return null;
  const p = str.trim().split('/');
  if (p.length === 3) { const d = new Date(`${p[2]}-${p[1].padStart(2,'0')}-${p[0].padStart(2,'0')}`); if (!isNaN(d)) return d; }
  const d2 = new Date(str); return isNaN(d2) ? null : d2;
}
const MONTHS_RMA = { Jan:0,Feb:1,Mar:2,Apr:3,May:4,Jun:5,Jul:6,Aug:7,Sep:8,Oct:9,Nov:10,Dec:11 };
function parseRmaDate(str) {
  if (!str) return null;
  const m = str.match(/(\d{2})-([A-Za-z]{3})-(\d{4})/);
  if (m) return new Date(parseInt(m[3]), MONTHS_RMA[m[2]], parseInt(m[1]));
  return parseDataBR(str);
}

// ════════════════════════════════════════════════════════════════════
// DESK — paginação e histórico
// ════════════════════════════════════════════════════════════════════
async function fetchDeskPages() {
  deskTickets = []; deskLoaded = false;
  document.getElementById('last-update').textContent = '⏳ Carregando Desk...';
  let from = 0, hasMore = true;
  while (hasMore) {
    try {
      const res = await fetch(`/api/desk-page?from=${from}`);
      const data = await res.json(); if (data.error) break;
      deskTickets = deskTickets.concat(data.tickets);
      hasMore = data.hasMore; from = data.nextFrom;
      document.getElementById('last-update').textContent = `⏳ Desk: ${deskTickets.length} tickets carregados...`;
      if (data.tickets.length > 0) loadReport();
    } catch { break; }
  }
  deskLoaded = true;
  await loadReport();
  document.getElementById('last-update').textContent = 'Atualizado: ' + new Date().toLocaleTimeString('pt-BR');
  fetchDeskHistory(deskTickets);
}

async function fetchDeskHistory(tickets) {
  if (historyLoading) return;
  historyLoading = true; historyLoaded = false;
  const ticketMap = {}; for (const t of tickets) ticketMap[String(t.ticketNumber)] = t;
  const rmaRaw = window._rmaRaw || [], dataCorte = new Date('2025-01-01');
  const rmaFiltrado = rmaRaw.filter(r => { const d = parseDataBR(r.addedTime); return d && d >= dataCorte; });
  const rmaNumeros = [...new Set(rmaFiltrado.map(r => String(r.deskNum||'').trim()).filter(n => n && n !== 'undefined' && n !== '0' && n.length > 3))];
  const comId = [], semId = [];
  for (const num of rmaNumeros) {
    const ck = 'rma_' + num;
    if (deskHistory[ck] || deskHistory[ticketMap[num]?.id]) continue;
    if (ticketMap[num]) comId.push(ticketMap[num]); else semId.push(num);
  }
  for (const t of tickets) {
    const num = String(t.ticketNumber);
    if (!rmaNumeros.includes(num) && (t.closedTime || STATUS_CONCLUIDOS.has(t.status)) && !deskHistory[t.id]) comId.push(t);
  }
  const total = comId.length + semId.length; let processed = 0;
  document.getElementById('last-update').textContent = `⏳ Histórico: 0/${total} tickets...`;

  for (const t of comId) {
    if (deskHistory[t.id]) { processed++; continue; }
    try {
      const res = await fetch(`/api/desk-history?ticketId=${t.id}`);
      const data = await res.json();
      if (!data.error && data.statusTimes && Object.keys(data.statusTimes).length > 0) {
        deskHistory[t.id] = { statusTimes: data.statusTimes, totalHours: t.totalHours||0, ticketNumber: t.ticketNumber, assigneeName: t.assigneeName, createdTime: t.createdTime, closedTime: t.closedTime||t.modifiedTime };
      }
    } catch {}
    processed++;
    if (processed % 10 === 0) {
      document.getElementById('last-update').textContent = `⏳ Histórico: ${processed}/${total} tickets...`;
      renderSlaReport();
      if (document.getElementById('tab-operacao')?.classList.contains('active') && window._lastReportData) renderOperacao(window._lastReportData);
    }
  }
  for (const num of semId) {
    const ck = 'rma_' + num; if (deskHistory[ck]) { processed++; continue; }
    try {
      const res = await fetch(`/api/desk-history?ticketNumber=${num}`);
      const data = await res.json();
      if (!data.error && data.statusTimes && Object.keys(data.statusTimes).length > 0) {
        deskHistory[ck] = { statusTimes: data.statusTimes, totalHours: 0, ticketNumber: num, assigneeName: 'Sem agente', createdTime: data.createdTime||'', closedTime: data.closedTime||'' };
      }
    } catch {}
    processed++;
    if (processed % 5 === 0) {
      document.getElementById('last-update').textContent = `⏳ Histórico: ${processed}/${total} tickets...`;
      renderSlaReport();
    }
  }
  historyLoading = false; historyLoaded = true;
  document.getElementById('last-update').textContent = 'Atualizado: ' + new Date().toLocaleTimeString('pt-BR');
  renderSlaReport();
  if (document.getElementById('tab-operacao')?.classList.contains('active') && window._lastReportData) renderOperacao(window._lastReportData);
}

// ════════════════════════════════════════════════════════════════════
// CARREGAMENTO PRINCIPAL
// ════════════════════════════════════════════════════════════════════
async function loadReport() {
  const from = document.getElementById('date-from').value, to = document.getElementById('date-to').value;
  try {
    const res = await fetch('/api/report', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ from, to, tickets: deskTickets }) });
    const data = await res.json();
    state.report = data;
    if (deskLoaded) document.getElementById('last-update').textContent = 'Atualizado: ' + new Date().toLocaleTimeString('pt-BR');
    if (data.rma?.raw) window._rmaRaw = data.rma.raw;
    if (data.rma?.rawFull) window._rmaRawFull = data.rma.rawFull;
    window._lastReportData = data;
    renderAll(data); renderSlaReport();
  } catch (e) { console.error('Erro ao carregar report:', e); }
}
async function loadData() {
  await loadReport();
  if (!deskLoaded && deskTickets.length === 0) fetchDeskPages();
  else if (deskLoaded) renderSlaReport();
}
function renderAll(d) {
  renderDataStatus(d.dataStatus);
  renderVisaoGeral(d);
  renderProdutos(d);
  renderOperacao(d);
  renderImportacoes(d);
  renderSourceStatus(d.dataStatus);
}

// ════════════════════════════════════════════════════════════════════
// ABA: VISÃO GERAL
// ════════════════════════════════════════════════════════════════════
function renderDataStatus(ds) {
  const el = document.getElementById('data-status');
  el.innerHTML = [{ key:'desk',label:'Desk'},{ key:'rma',label:'RMA'},{ key:'sankhya',label:'Sankhya'},{ key:'spareParts',label:'Spare Parts'}]
    .map(i => `<div class="ds-badge ${ds[i.key]?'ok':'missing'}">${ds[i.key]?'●':'○'} ${i.label}</div>`).join('');
}

function renderVisaoGeral(d) {
  const r = d.rma, desk = d.desk;
  document.getElementById('kpi-geral').innerHTML = [
    { val: r.total, lbl: 'RMA no Período', cls: 'yellow' },
    { val: desk.total, lbl: 'Tickets Desk Abertos', cls: 'blue' },
    { val: r.warrantyCount.warranty, lbl: 'Em Garantia', cls: 'green' },
    { val: r.warrantyCount.noWarranty + r.warrantyCount.noWarrantyMaint, lbl: 'Fora de Garantia', cls: 'red' },
    { val: Object.keys(r.lineCount).length, lbl: 'Linhas de Produto', cls: 'purple' },
    { val: d.spareParts.filter(p => p.alert).length, lbl: 'Alertas de Estoque', cls: 'orange' }
  ].map(k => `<div class="kpi ${k.cls}"><div class="val">${k.val}</div><div class="lbl">${k.lbl}</div></div>`).join('');

  const lines = Object.entries(r.lineCount).sort((a,b) => b[1]-a[1]);
  makeChart('chart-line', { type:'doughnut', data:{ labels:lines.map(l=>l[0]), datasets:[{ data:lines.map(l=>l[1]), backgroundColor:palette(lines.length), borderWidth:2, borderColor:C.bg2 }] }, options:{ plugins:{ legend:{ position:'right', labels:{ boxWidth:12, font:{ size:12 }}} }, cutout:'60%' }});

  const statusEntries = Object.entries(desk.slaByStatus).sort((a,b) => b[1].total-a[1].total);
  makeChart('chart-desk-status', { type:'bar', data:{ labels:statusEntries.map(([s]) => s.replace('Aguardando','Ag.').replace('Manutenção','Manut.')), datasets:[{ label:'OK', data:statusEntries.map(([,v])=>v.ok), backgroundColor:C.green, borderRadius:3 },{ label:'Atenção', data:statusEntries.map(([,v])=>v.atencao), backgroundColor:C.orange, borderRadius:3 },{ label:'Vencido', data:statusEntries.map(([,v])=>v.vencido), backgroundColor:C.red, borderRadius:3 }] }, options:{ indexAxis:'y', scales:{ x:{ stacked:true }, y:{ stacked:true } }, plugins:{ legend:{ position:'bottom' }}}});

  const w = r.warrantyCount;
  makeChart('chart-warranty', { type:'pie', data:{ labels:['Em Garantia','Fora de Garantia','Manutenção s/ Garantia'], datasets:[{ data:[w.warranty,w.noWarranty,w.noWarrantyMaint], backgroundColor:[C.green,C.red,C.purple], borderWidth:2, borderColor:C.bg2 }] }, options:{ plugins:{ legend:{ position:'bottom', labels:{ boxWidth:12, font:{ size:11 }}}}}});

  const svc = Object.entries(r.serviceCount).sort((a,b) => b[1]-a[1]).slice(0,8);
  makeChart('chart-service', { type:'doughnut', data:{ labels:svc.map(([s])=>s), datasets:[{ data:svc.map(([,v])=>v), backgroundColor:palette(svc.length), borderWidth:2, borderColor:C.bg2 }] }, options:{ plugins:{ legend:{ position:'bottom', labels:{ boxWidth:12, font:{ size:11 }}}}, cutout:'55%' }});

  const tot = Object.values(desk.slaByStatus).reduce((a,v) => ({ ok:a.ok+v.ok, atencao:a.atencao+v.atencao, vencido:a.vencido+v.vencido, total:a.total+v.total }), { ok:0,atencao:0,vencido:0,total:0 });
  const pct = n => tot.total > 0 ? Math.round(n/tot.total*100) : 0;
  document.getElementById('sla-summary').innerHTML = `<div style="margin-bottom:12px;">${[['OK',tot.ok,C.green],['Atenção',tot.atencao,C.orange],['Vencido',tot.vencido,C.red]].map(([lbl,val,col]) => `<div style="display:flex;justify-content:space-between;margin-bottom:8px;"><span style="color:${col};font-size:13px;">● ${lbl}</span><span style="font-family:'Barlow Condensed',sans-serif;font-size:18px;font-weight:700;">${val} <span style="font-size:12px;color:var(--gray2);">(${pct(val)}%)</span></span></div><div class="prog-bar"><div class="fill ${lbl.toLowerCase().replace('ã','a')}" style="width:${pct(val)}%"></div></div>`).join('')}</div><div style="font-size:12px;color:var(--gray2);margin-top:8px;">Total: ${tot.total} tickets</div>`;
}

// ════════════════════════════════════════════════════════════════════
// ABA: PRODUTOS
// ════════════════════════════════════════════════════════════════════
function setProdGarantia(tipo, btn) {
  prodGarantiaFiltro = tipo; prodSelecionado = null;
  document.querySelectorAll('.filtro-garantia-btns button').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  if (window._lastReportData) renderProdutos(window._lastReportData);
}
function getRmaFiltradoProd(raw) {
  if (prodGarantiaFiltro === 'todos') return raw;
  return raw.filter(r => {
    const v = (r.validation || '').toLowerCase();
    if (prodGarantiaFiltro === 'garantia') return !v.includes('no warranty');
    if (prodGarantiaFiltro === 'fora') return v.includes('no warranty') && !v.includes('maintenance');
    if (prodGarantiaFiltro === 'manutencao') return v.includes('no warranty maintenance');
    return true;
  });
}
function getRmaPeriodo() {
  const from = new Date(document.getElementById('date-from').value);
  const to = new Date(document.getElementById('date-to').value); to.setHours(23,59,59);
  const raw = window._rmaRawFull || window._rmaRaw || [];
  return getRmaFiltradoProd(raw).filter(r => { const d = parseRmaDate(r.addedTime); return d && d >= from && d <= to; });
}
function selecionarProd(model) { prodSelecionado = prodSelecionado === model ? null : model; renderTopProducts(); renderTopFaults(); }
function clearProdSel() { prodSelecionado = null; renderTopProducts(); renderTopFaults(); }

function renderTopProducts() {
  const dados = getRmaPeriodo(), contagem = {};
  for (const r of dados) { const m = (r.model||'Desconhecido').trim(); contagem[m] = (contagem[m]||0) + 1; }
  const top = Object.entries(contagem).sort((a,b) => b[1]-a[1]).slice(0,10), max = top[0]?.[1] || 1;
  const countEl = document.getElementById('prod-filter-count');
  if (countEl) countEl.textContent = `${dados.length} atendimento${dados.length!==1?'s':''} no período`;
  const el = document.getElementById('top-products-list'); if (!el) return;
  if (!top.length) { el.innerHTML = '<div class="empty"><div class="icon">📭</div><p>Sem dados RMA no período</p></div>'; return; }
  el.innerHTML = top.map(([model, count]) => {
    const sel = prodSelecionado === model, pct = Math.round(count/max*100), label = model.length > 52 ? model.substring(0,52)+'…' : model;
    return `<div class="hbar-item-click ${sel?'selected':''}" onclick="selecionarProd('${model.replace(/\\/g,'\\\\').replace(/'/g,"\\'")}')"><div class="row"><span class="nome" title="${model}">${label}</span><strong>${count}</strong></div><div class="hbar-track"><div class="hbar-fill" style="width:${pct}%"></div></div></div>`;
  }).join('');
}

function renderTopFaults() {
  const dados = getRmaPeriodo().filter(r => !prodSelecionado || (r.model||'').trim() === prodSelecionado), contagem = {};
  for (const r of dados) { const f = (r.fault||r.defect||r.issue||'').trim(); if (!f||f==='—'||f==='-') continue; contagem[f] = (contagem[f]||0) + 1; }
  const top = Object.entries(contagem).sort((a,b) => b[1]-a[1]).slice(0,10), max = top[0]?.[1] || 1;
  const painel = document.getElementById('defeitos-panel'), badge = document.getElementById('defeitos-badge');
  if (prodSelecionado) { painel.classList.add('has-selection'); badge.style.display = 'inline-block'; badge.textContent = prodSelecionado.length > 28 ? prodSelecionado.substring(0,28)+'…' : prodSelecionado; badge.title = prodSelecionado; }
  else { painel.classList.remove('has-selection'); badge.style.display = 'none'; }
  const el = document.getElementById('top-faults-list'); if (!el) return;
  if (!top.length) { el.innerHTML = `<div class="defeitos-empty"><div class="ico">${prodSelecionado?'🔍':'←'}</div><p>${prodSelecionado?'Nenhum defeito registrado para este produto no período':'Selecione um produto para ver os defeitos específicos'}</p></div>`; return; }
  el.innerHTML = `<div class="fade-up">${top.map(([fault,count]) => { const pct = Math.round(count/max*100), label = fault.length > 45 ? fault.substring(0,45)+'…' : fault; return `<div class="hbar-item-click" style="cursor:default;"><div class="row"><span class="nome" title="${fault}">${label}</span><strong>${count}</strong></div><div class="hbar-track"><div class="hbar-fill red" style="width:${pct}%"></div></div></div>`; }).join('')}</div>`;
}

function renderProdutos(d) {
  renderTopProducts(); renderTopFaults();
  const r = d.rma, lines = Object.entries(r.lineCount).sort((a,b) => b[1]-a[1]);
  makeChart('chart-line2', { type:'bar', data:{ labels:lines.map(l=>l[0]), datasets:[{ label:'Atendimentos', data:lines.map(l=>l[1]), backgroundColor:palette(lines.length), borderRadius:4 }] }, options:{ maintainAspectRatio:false, plugins:{ legend:{ display:false }, tooltip:{ enabled:false } }, scales:{ x:{ ticks:{ maxRotation:45, font:{ size:10 }}}, y:{ beginAtZero:true }}}, plugins:[{ id:'lineLabels', afterDatasetsDraw(chart) { const {ctx,data}=chart; chart.getDatasetMeta(0).data.forEach((bar,i) => { const val=data.datasets[0].data[i]; if (!val) return; ctx.save(); ctx.font='bold 10px Barlow Condensed, sans-serif'; ctx.fillStyle='#FFFDF0'; ctx.textAlign='center'; ctx.textBaseline='bottom'; ctx.fillText(val,bar.x,bar.y-3); ctx.restore(); }); }}]});

  const trimestralRaw = d.rma?.trimestral || {};
  function getServCat(service) {
    const s = (service||'').trim();
    if (s==='Flawless'||s==='Firmware Update') return 'A - Sem Defeito';
    if (s==='Analyze'||s==='Software upgrade') return 'B - Sem Garantia (Serviço não aprovado)';
    if (s==='Recharge') return 'C - Recarga Bateria';
    if (s==='PCB Replacement'||s==='PCB replacement') return 'D - Subst. PCB';
    if (s==='Component Replacement'||s==='Component Replacement - dismantle'||s==='Component Maintenance') return 'E - Subst. Componentes';
    if (s==='Product Exchange') return 'F - Troca de Produto';
    return 'B - Sem Garantia (Serviço não aprovado)';
  }
  const servicosCats = ['A - Sem Defeito','B - Sem Garantia (Serviço não aprovado)','C - Recarga Bateria','D - Subst. PCB','E - Subst. Componentes','F - Troca de Produto'];
  const servicoCores = ['#52c878','#5299e0','#e09052','#FFD04C','#9b59e0','#e05252'];
  const trimestreData = {}, garantiaTrimestre = {};
  for (const [trim, data] of Object.entries(trimestralRaw)) {
    trimestreData[trim] = {}; garantiaTrimestre[trim] = { warranty:data.warranty||0, noWarranty:data.noWarranty||0, maintenance:data.maintenance||0 };
    for (const [svc, count] of Object.entries(data.services||{})) { const cat = getServCat(svc); trimestreData[trim][cat] = (trimestreData[trim][cat]||0) + count; }
  }
  const trimestres = Object.keys(trimestreData).sort((a,b) => {
    if (a.includes('/')&&b.includes('/')) { const[ma,ya]=a.split('/');const[mb,yb]=b.split('/'); return ya!==yb?Number(ya)-Number(yb):Number(ma)-Number(mb); }
    const[qa,ya2]=a.split(' ');const[qb,yb2]=b.split(' '); return ya2!==yb2?Number(ya2)-Number(yb2):qa.localeCompare(qb);
  });

  // Local de teste
  const rmaParaLoc = window._rmaRawFull||window._rmaRaw||[];
  const fromDateLoc = new Date(document.getElementById('date-from').value), toDateLoc = new Date(document.getElementById('date-to').value); toDateLoc.setHours(23,59,59);
  const locPorPeriodo = {};
  for (const r of rmaParaLoc) {
    const d2 = parseRmaDate(r.addedTime); if (!d2||d2<fromDateLoc||d2>toDateLoc) continue;
    const loc = (r.testLocation||'Não informado').trim()||'Não informado', diff = (toDateLoc-fromDateLoc)/(1000*60*60*24);
    const periodo = diff<=92 ? `${String(d2.getMonth()+1).padStart(2,'0')}/${d2.getFullYear()}` : `T${Math.ceil((d2.getMonth()+1)/3)} ${d2.getFullYear()}`;
    if (!locPorPeriodo[periodo]) locPorPeriodo[periodo] = {};
    locPorPeriodo[periodo][loc] = (locPorPeriodo[periodo][loc]||0) + 1;
  }
  const locPeriodos = Object.keys(locPorPeriodo).sort((a,b) => {
    if (a.includes('/')&&b.includes('/')) { const[ma,ya]=a.split('/');const[mb,yb]=b.split('/'); return ya!==yb?Number(ya)-Number(yb):Number(ma)-Number(mb); }
    const[qa,ya]=a.split(' ');const[qb,yb]=b.split(' '); return ya!==yb?Number(ya)-Number(yb):Number(qa.replace('T',''))-Number(qb.replace('T',''));
  });
  const allLocs = [...new Set(Object.values(locPorPeriodo).flatMap(p => Object.keys(p)))].sort(), locCores = [C.yellow,C.blue,C.green,C.orange,C.purple,C.red];
  if (locPeriodos.length>0&&allLocs.length>0) makeChart('chart-test-location', { type:'bar', data:{ labels:locPeriodos, datasets:allLocs.map((loc,i) => ({ label:loc, data:locPeriodos.map(p=>locPorPeriodo[p]?.[loc]||0), backgroundColor:locCores[i%locCores.length], borderRadius:2 })) }, options:{ maintainAspectRatio:false, plugins:{ legend:{ position:'bottom', labels:{ boxWidth:12, color:C.white }}}, scales:{ x:{ stacked:true, ticks:{ color:C.gray2, maxRotation:45 }}, y:{ stacked:true, beginAtZero:true, ticks:{ color:C.gray2 }}}}});

  const servicoDatasets = (mode) => servicosCats.map((cat,i) => ({ label:cat, data:trimestres.map(t => { if (mode==='pct') { const total=Object.values(trimestreData[t]||{}).reduce((s,v)=>s+v,0); return total>0?Math.round((trimestreData[t]?.[cat]||0)/total*100):0; } return trimestreData[t]?.[cat]||0; }), backgroundColor:servicoCores[i], borderColor:servicoCores[i], borderRadius:2, stack:'stack' }));
  const segPlugin = (suffix) => ({ id:'seg'+suffix, afterDatasetsDraw(chart) { const {ctx,data}=chart; data.datasets.forEach((ds,di) => { const meta=chart.getDatasetMeta(di); meta.data.forEach((bar,i) => { const val=ds.data[i]; if (!val||val<(suffix==='Pct'?5:3)) return; const bh=Math.abs(bar.base-bar.y); if (bh<14) return; ctx.save(); ctx.font='bold 10px Barlow Condensed, sans-serif'; ctx.fillStyle='#1c1e20'; ctx.textAlign='center'; ctx.textBaseline='middle'; ctx.fillText(val+(suffix==='Pct'?'%':''),bar.x,bar.y+bh/2); ctx.restore(); }); }); }});
  makeChart('chart-servico-abs', { type:'bar', data:{ labels:trimestres, datasets:servicoDatasets('abs') }, options:{ maintainAspectRatio:false, plugins:{ legend:{ position:'right', labels:{ boxWidth:12, font:{ size:11 }, color:C.white }}}, scales:{ x:{ stacked:true, ticks:{ color:C.gray2, maxRotation:45 }}, y:{ stacked:true, beginAtZero:true, ticks:{ color:C.gray2 }}}}, plugins:[segPlugin('')]});
  makeChart('chart-servico-pct', { type:'bar', data:{ labels:trimestres, datasets:servicoDatasets('pct') }, options:{ maintainAspectRatio:false, plugins:{ legend:{ position:'right', labels:{ boxWidth:12, font:{ size:11 }, color:C.white }}}, scales:{ x:{ stacked:true, ticks:{ color:C.gray2, maxRotation:45 }}, y:{ stacked:true, max:100, ticks:{ color:C.gray2, callback:v=>v+'%' }}}}, plugins:[segPlugin('Pct')]});

  const totalW=Object.values(garantiaTrimestre).reduce((s,v)=>s+v.warranty,0), totalNW=Object.values(garantiaTrimestre).reduce((s,v)=>s+v.noWarranty,0), totalM=Object.values(garantiaTrimestre).reduce((s,v)=>s+v.maintenance,0), totalG=totalW+totalNW+totalM;
  makeChart('chart-garantia-donut', { type:'doughnut', data:{ labels:['Garantia','Sem Garantia','Manutenção Paga'], datasets:[{ data:[totalW,totalNW,totalM], backgroundColor:[C.yellow,C.green,C.orange], borderWidth:2, borderColor:C.bg2 }] }, options:{ maintainAspectRatio:false, cutout:'60%', centerText:{ line1:totalG.toString(), line2:'Total de Chamados' }, plugins:{ legend:{ display:false }, tooltip:{ enabled:false }}}, plugins:[{ id:'donutLabels', afterDatasetsDraw(chart) { const {ctx}=chart,dataset=chart.data.datasets[0],total=dataset.data.reduce((a,b)=>a+b,0); chart.getDatasetMeta(0).data.forEach((arc,i) => { const val=dataset.data[i]; if (!val||val/total<0.05) return; const angle=(arc.startAngle+arc.endAngle)/2,r=(arc.innerRadius+arc.outerRadius)/2,x=arc.x+Math.cos(angle)*r,y=arc.y+Math.sin(angle)*r,pct=Math.round(val/total*100); ctx.save(); ctx.textAlign='center'; ctx.textBaseline='middle'; ctx.font='bold 13px Barlow Condensed, sans-serif'; ctx.fillStyle='#1c1e20'; ctx.fillText(pct+'%',x,y-7); ctx.font='10px Barlow, sans-serif'; ctx.fillText(val,x,y+7); ctx.restore(); }); }}]});
  const legendEl = document.getElementById('donut-garantia-legenda');
  if (legendEl) legendEl.innerHTML = [{ label:'Garantia', val:totalW, color:C.yellow },{ label:'Sem Garantia', val:totalNW, color:C.green },{ label:'Manutenção Paga', val:totalM, color:C.orange }].map(l => `<span style="display:inline-flex;align-items:center;gap:5px;margin:0 10px;"><span style="width:10px;height:10px;border-radius:2px;background:${l.color};display:inline-block;"></span><span style="font-size:11px;color:var(--gray2);">${l.label} (${totalG>0?Math.round(l.val/totalG*100):0}% — ${l.val})</span></span>`).join('');
  makeChart('chart-garantia-periodo', { type:'bar', data:{ labels:trimestres, datasets:[{ label:'Garantia', data:trimestres.map(t=>garantiaTrimestre[t]?.warranty||0), backgroundColor:C.yellow, borderRadius:2, stack:'g' },{ label:'Sem Garantia', data:trimestres.map(t=>garantiaTrimestre[t]?.noWarranty||0), backgroundColor:C.green, borderRadius:2, stack:'g' },{ label:'Manutenção Paga', data:trimestres.map(t=>garantiaTrimestre[t]?.maintenance||0), backgroundColor:C.orange, borderRadius:2, stack:'g' }] }, options:{ maintainAspectRatio:false, plugins:{ legend:{ position:'bottom', labels:{ boxWidth:12, color:C.white }}}, scales:{ x:{ stacked:true, ticks:{ color:C.gray2, maxRotation:45 }}, y:{ stacked:true, beginAtZero:true, ticks:{ color:C.gray2 }}}}, plugins:[segPlugin('G')]});
}

// ════════════════════════════════════════════════════════════════════
// ABA: SLA & LEAD TIME
// ════════════════════════════════════════════════════════════════════
function renderSlaReport() {
  const filtroGarantia = document.getElementById('filtro-garantia')?.value || 'todos';
  const rmaMap = {};
  (window._rmaRaw||[]).forEach(r => {
    const v = r.validation?.toLowerCase()||''; let tipo = 'warranty';
    if (v.includes('no warranty maintenance')) tipo = 'maintenance';
    else if (v.includes('no warranty')) tipo = 'no_warranty';
    if (r.deskNum) rmaMap[String(r.deskNum).trim()] = tipo;
  });
  const from = new Date(document.getElementById('date-from').value), to = new Date(document.getElementById('date-to').value); to.setHours(23,59,59);
  const histEntries = Object.values(deskHistory).filter(h => {
    if (!h.statusTimes||Object.keys(h.statusTimes).length===0) return false;
    const d = new Date(h.createdTime); if (d<from||d>to) return false;
    if (filtroGarantia==='todos') return true;
    const tipo = rmaMap[String(h.ticketNumber)]||'warranty';
    if (filtroGarantia==='garantia') return tipo==='warranty';
    if (filtroGarantia==='fora') return tipo==='no_warranty';
    if (filtroGarantia==='manutencao') return tipo==='maintenance';
    return true;
  });
  const total = histEntries.length, statusMetrics = {};
  for (const h of histEntries) {
    for (const [status, hours] of Object.entries(h.statusTimes||{})) {
      if (!statusMetrics[status]) statusMetrics[status] = { totalDias:0, ticketsUnicos:new Set(), dentroSla:0 };
      const dias = horasParaDiasUteis(hours, h.createdTime), slaDias = SLA_DIAS_UTEIS[status];
      statusMetrics[status].totalDias += dias; statusMetrics[status].ticketsUnicos.add(h.ticketNumber);
      if (!slaDias||dias<=slaDias) statusMetrics[status].dentroSla++;
    }
  }
  const grupos = ['ENTRADA','AVALIAÇÃO','PAGAMENTO','TÉCNICO','DEVOLUÇÃO'];
  const tblEl = document.getElementById('tbody-sla-hist'); if (!tblEl) return;
  let rows = '';
  for (const grupo of grupos) {
    const statusDoGrupo = Object.entries(statusMetrics).filter(([s]) => STATUS_GRUPOS[s]===grupo); if (statusDoGrupo.length===0) continue;
    const ticketsDoGrupo = new Set(); for (const [,v] of statusDoGrupo) v.ticketsUnicos.forEach(t => ticketsDoGrupo.add(t));
    const grpUnico=ticketsDoGrupo.size, grpSlaSoma=statusDoGrupo.reduce((a,[s])=>a+(SLA_DIAS_UTEIS[s]||0),0), grpTotalOcorr=statusDoGrupo.reduce((a,[,v])=>a+v.ticketsUnicos.size,0), grpDias=statusDoGrupo.reduce((a,[,v])=>a+v.totalDias,0), grpDentro=statusDoGrupo.reduce((a,[,v])=>a+v.dentroSla,0);
    const grpMediaDias=grpTotalOcorr>0?grpDias/grpTotalOcorr:0, grpSla=grpTotalOcorr>0?Math.round(grpDentro/grpTotalOcorr*100):0, grpFreq=total>0?Math.round(grpUnico/total*100):0, grpColor=grpSla>=80?C.green:grpSla>=50?C.orange:C.red;
    rows += `<tr style="background:rgba(255,255,255,0.05);"><td style="font-family:'Barlow Condensed',sans-serif;font-weight:700;font-size:13px;color:var(--yellow);">${grupo}</td><td class="num" style="font-weight:600;">${grpMediaDias.toFixed(2)}</td><td class="num"><span style="color:${grpColor};font-weight:600;">${grpSla}%</span></td><td class="num" style="font-weight:600;">${grpSlaSoma>0?grpSlaSoma.toFixed(2):'—'}</td><td class="num">${grpUnico}</td><td class="num">${grpFreq}%</td></tr>`;
    for (const [status, v] of statusDoGrupo.sort((a,b) => b[1].ticketsUnicos.size-a[1].ticketsUnicos.size)) {
      const unicos=v.ticketsUnicos.size, mediaDias=unicos>0?v.totalDias/unicos:0, pctSla=unicos>0?Math.round(v.dentroSla/unicos*100):0, freq=total>0?Math.round(unicos/total*100):0, slaDias=SLA_DIAS_UTEIS[status]||null, color=pctSla>=80?C.green:pctSla>=50?C.orange:C.red;
      rows += `<tr><td style="padding-left:20px;color:var(--gray2);">${status}</td><td class="num">${mediaDias.toFixed(2)}</td><td class="num"><span style="color:${color};">● ${pctSla}%</span></td><td class="num">${slaDias!==null?slaDias:'—'}</td><td class="num">${unicos}</td><td class="num">${freq}%</td></tr>`;
    }
  }
  tblEl.innerHTML = rows || `<tr><td colspan="6" style="text-align:center;color:var(--gray2);">${Object.keys(deskHistory).length===0?'⏳ Carregando histórico de tickets...':'Sem dados para o período/filtro selecionado'}</td></tr>`;

  // Histogramas
  const bucketLabels = ['0-1','1-2','2-4','4-6','6-8','8-10','10-12','12-14','14-16','16-18','18-20','20-22','22-24','24-26','26-28','28-30','30-32','32-34','34-36','36-38','38-40','40-48','48-100','100+'];
  function getBucket(d) { if(d<1)return 0;if(d<2)return 1;if(d<4)return 2;if(d<6)return 3;if(d<8)return 4;if(d<10)return 5;if(d<12)return 6;if(d<14)return 7;if(d<16)return 8;if(d<18)return 9;if(d<20)return 10;if(d<22)return 11;if(d<24)return 12;if(d<26)return 13;if(d<28)return 14;if(d<30)return 15;if(d<32)return 16;if(d<34)return 17;if(d<36)return 18;if(d<38)return 19;if(d<40)return 20;if(d<48)return 21;if(d<100)return 22;return 23; }
  const histData = {}; for (const g of grupos) histData[g] = new Array(bucketLabels.length).fill(0);
  for (const h of histEntries) { for (const [status,hours] of Object.entries(h.statusTimes||{})) { const grupo=STATUS_GRUPOS[status]; if (!grupo||!histData[grupo]) continue; histData[grupo][getBucket(horasParaDiasUteis(hours,h.createdTime))]++; }}
  const histColors = { ENTRADA:C.green, AVALIAÇÃO:C.blue, PAGAMENTO:'#e0c052', TÉCNICO:C.orange, DEVOLUÇÃO:C.purple };
  const histIds = { ENTRADA:'chart-hist-entrada', AVALIAÇÃO:'chart-hist-avaliacao', PAGAMENTO:'chart-hist-pagamento', TÉCNICO:'chart-hist-tecnico', DEVOLUÇÃO:'chart-hist-devolucao' };
  for (const g of grupos) { if (!document.getElementById(histIds[g])) continue; const dados=histData[g]||new Array(bucketLabels.length).fill(0); let last=dados.length-1; while(last>3&&dados[last]===0)last--; makeChart(histIds[g], { type:'bar', data:{ labels:bucketLabels.slice(0,last+2), datasets:[{ label:g, data:dados.slice(0,last+2), backgroundColor:histColors[g]||C.gray, borderRadius:3 }] }, options:{ maintainAspectRatio:false, plugins:{ legend:{ display:false }}, scales:{ x:{ ticks:{ font:{ size:9 }, maxRotation:45 }}, y:{ beginAtZero:true, ticks:{ font:{ size:10 }}}}}}); }

  // SLA por mês
  const slaByMonth = {};
  for (const h of histEntries) {
    const month = h.createdTime?.substring(0,7)||'N/A';
    if (!slaByMonth[month]) slaByMonth[month] = { ENTRADA:{d:0,t:0}, AVALIAÇÃO:{d:0,t:0}, TÉCNICO:{d:0,t:0}, PAGAMENTO:{d:0,t:0}, DEVOLUÇÃO:{d:0,t:0} };
    for (const [status,hours] of Object.entries(h.statusTimes||{})) { const grupo=STATUS_GRUPOS[status]; if (!grupo||!slaByMonth[month][grupo]) continue; const dias=horasParaDiasUteis(hours,h.createdTime),slaDias=SLA_DIAS_UTEIS[status]; slaByMonth[month][grupo].t++; if (!slaDias||dias<=slaDias) slaByMonth[month][grupo].d++; }
  }
  const months = Object.keys(slaByMonth).sort(), grupoColors = { ENTRADA:C.green, AVALIAÇÃO:C.blue, TÉCNICO:C.orange, PAGAMENTO:'#e0c052', DEVOLUÇÃO:C.purple };
  makeChart('chart-sla-mensal', { type:'bar', data:{ labels:months.map(m => { const[y,mo]=m.split('-'); return new Date(y,mo-1).toLocaleString('pt-BR',{month:'short',year:'2-digit'}); }), datasets:[...grupos.map(g => ({ label:g, data:months.map(m => { const v=slaByMonth[m]?.[g]; return v&&v.t>0?Math.round(v.d/v.t*100):null; }), backgroundColor:grupoColors[g], borderRadius:3 })),{ label:'Meta (80%)', data:months.map(()=>80), type:'line', borderColor:C.yellow, borderDash:[5,5], pointRadius:0, borderWidth:2, fill:false }] }, options:{ maintainAspectRatio:false, plugins:{ legend:{ position:'bottom' }, datalabels:{ display:false }}, scales:{ x:{ ticks:{ maxRotation:45, minRotation:0 }}, y:{ beginAtZero:true, max:105, ticks:{ callback:v=>v+'%' }}}}});

  // KPIs de SLA
  const allDentro=Object.values(statusMetrics).reduce((a,v)=>a+v.dentroSla,0), allTotal=Object.values(statusMetrics).reduce((a,v)=>a+v.ticketsUnicos.size,0), pctGeral=allTotal>0?Math.round(allDentro/allTotal*100):0;
  const elSlaGeral=document.getElementById('kpi-sla-geral'); if (elSlaGeral) { const color=pctGeral>=80?C.green:pctGeral>=50?C.orange:C.red; elSlaGeral.innerHTML=`<div class="val" style="color:${color}">${pctGeral}%</div><div class="lbl">% Conclusão SLA Geral</div>`; }
  const elSlaTickets=document.getElementById('kpi-sla-tickets'); if (elSlaTickets) elSlaTickets.innerHTML=`<div class="val">${total}</div><div class="lbl">Tickets com Histórico</div>`;
  const elSlaLoading=document.getElementById('kpi-sla-loading'); if (elSlaLoading) { const tf=deskTickets.filter(t=>t.closedTime).length; elSlaLoading.innerHTML=`<div class="val" style="font-size:16px;">${Object.keys(deskHistory).length}/${tf}</div><div class="lbl">Histórico Carregado</div>`; }
}

function abrirValidacao() {
  const painel = document.getElementById('painel-validacao'); painel.style.display = 'block';
  painel.scrollIntoView({ behavior:'smooth', block:'start' });
  const sel = document.getElementById('validacao-status');
  const statusList = [...new Set(Object.values(deskHistory).flatMap(h => Object.keys(h.statusTimes||{})))].sort();
  sel.innerHTML = '<option value="">Todos os status</option>' + statusList.map(s => `<option value="${s}">${s}</option>`).join('');
  renderValidacaoTabela();
}
function renderValidacaoTabela() {
  const filtroStatus = document.getElementById('validacao-status')?.value||'';
  const from = new Date(document.getElementById('date-from').value), to = new Date(document.getElementById('date-to').value); to.setHours(23,59,59);
  const rows = [];
  for (const h of Object.values(deskHistory)) {
    const d = new Date(h.createdTime); if (d<from||d>to) continue;
    for (const [status,hours] of Object.entries(h.statusTimes||{})) {
      if (filtroStatus&&status!==filtroStatus) continue;
      const grupo=STATUS_GRUPOS[status]||'—', dias=horasParaDiasUteis(hours,h.createdTime), slaDias=SLA_DIAS_UTEIS[status]||null, dentroSla=!slaDias||dias<=slaDias;
      rows.push({ ticket:h.ticketNumber, status, grupo, hours, dias, slaDias, dentroSla, createdTime:h.createdTime });
    }
  }
  rows.sort((a,b) => Number(b.ticket)-Number(a.ticket));
  const tbody = document.getElementById('tbody-validacao');
  if (!rows.length) { tbody.innerHTML='<tr><td colspan="8" style="text-align:center;color:var(--gray2);padding:20px;">Nenhum dado encontrado</td></tr>'; return; }
  tbody.innerHTML = rows.slice(0,500).map(r => {
    const cor=r.dentroSla?'var(--green)':'var(--red)', resultado=r.dentroSla?'✓ OK':'✗ Vencido', dataStr=r.createdTime?new Date(r.createdTime).toLocaleDateString('pt-BR'):'—';
    return `<tr><td><strong>#${r.ticket}</strong></td><td>${r.status}</td><td style="color:var(--yellow);font-weight:600;">${r.grupo}</td><td class="num">${r.hours.toFixed(1)}h</td><td class="num">${r.dias.toFixed(2)}</td><td class="num">${r.slaDias??'—'}</td><td class="num" style="color:${cor};font-weight:600;">${resultado}</td><td>${dataStr}</td></tr>`;
  }).join('');
}

// ════════════════════════════════════════════════════════════════════
// ABA: OPERAÇÃO
// ════════════════════════════════════════════════════════════════════
function renderOperacao(d) {
  const rmaByDesk = {}, allRma = window._rmaRawFull||window._rmaRaw||[];
  for (const r of allRma) { const num=String(r.deskNum||'').trim(); if (num&&num!=='0') rmaByDesk[num]=r; }
  const from = new Date(document.getElementById('date-from').value), to = new Date(document.getElementById('date-to').value); to.setHours(23,59,59);
  const ticketsCusto=[], custoPorLinha={}, custoPorPeriodo={}, custoPorProduto={}; let totalHorasUteis=0, totalCusto=0;
  for (const h of Object.values(deskHistory)) {
    const d2 = new Date(h.createdTime); if (d2<from||d2>to) continue;
    const ticketNum=String(h.ticketNumber||'').replace('rma_','').trim(), rma=rmaByDesk[ticketNum]; if (!rma) continue;
    let horasTicket=0; const statusCustoList=[];
    for (const [status,hours] of Object.entries(h.statusTimes||{})) {
      if (!STATUS_CUSTO.has(status)) continue;
      const inicio=new Date(h.createdTime), fim=new Date(inicio.getTime()+hours*3600000);
      horasTicket += horasUteisEntre(inicio, fim); statusCustoList.push(status);
    }
    if (horasTicket <= 0) continue;
    const custo=horasTicket*CUSTO_POR_HORA; totalHorasUteis+=horasTicket; totalCusto+=custo;
    const linha=rma.fornecedor||rma.fornecedorNome||'Não identificado', produto=rma.model||'Não identificado', garantia=rma.validation||'—';
    custoPorLinha[linha] = (custoPorLinha[linha]||0) + custo;
    const prod = produto.length>50 ? produto.substring(0,50)+'...' : produto;
    if (!custoPorProduto[prod]) custoPorProduto[prod] = { total:0, count:0 };
    custoPorProduto[prod].total += custo; custoPorProduto[prod].count++;
    const mes = d2.toLocaleDateString('pt-BR',{ month:'short', year:'2-digit' }); custoPorPeriodo[mes] = (custoPorPeriodo[mes]||0) + custo;
    ticketsCusto.push({ ticket:h.ticketNumber, produto:prod, fornecedor:linha, garantia, horasUteis:horasTicket, custo, statusList:[...new Set(statusCustoList)].join(', '), data:h.createdTime });
  }
  ticketsCusto.sort((a,b) => b.custo-a.custo);
  const ticketsComCusto=ticketsCusto.length, custoMedio=ticketsComCusto>0?totalCusto/ticketsComCusto:0;
  document.getElementById('kpi-custo').innerHTML = [
    { val:`R$ ${totalCusto.toLocaleString('pt-BR',{maximumFractionDigits:0})}`, lbl:'Custo Total Estimado', cls:'red' },
    { val:ticketsComCusto, lbl:'Tickets com Custo', cls:'blue' },
    { val:`${totalHorasUteis.toFixed(1)}h`, lbl:'Horas Úteis Técnicos', cls:'yellow' },
    { val:`R$ ${custoMedio.toLocaleString('pt-BR',{maximumFractionDigits:0})}`, lbl:'Custo Médio por Ticket', cls:'orange' },
    { val:Object.keys(custoPorLinha).length, lbl:'Linhas de Produto', cls:'purple' }
  ].map(k => `<div class="kpi ${k.cls}"><div class="val" style="font-size:18px;">${k.val}</div><div class="lbl">${k.lbl}</div></div>`).join('');
  const linhas = Object.entries(custoPorLinha).sort((a,b)=>b[1]-a[1]).slice(0,12);
  makeChart('chart-custo-linha', { type:'bar', data:{ labels:linhas.map(([l])=>l), datasets:[{ label:'Custo R$', data:linhas.map(([,v])=>Math.round(v)), backgroundColor:palette(linhas.length), borderRadius:4 }] }, options:{ maintainAspectRatio:false, indexAxis:'y', plugins:{ legend:{ display:false }, tooltip:{ enabled:false }}, scales:{ x:{ beginAtZero:true, ticks:{ callback:v=>'R$'+v.toLocaleString('pt-BR') }}}}});
  const periodos = Object.entries(custoPorPeriodo).sort((a,b)=>a[0].localeCompare(b[0]));
  makeChart('chart-custo-periodo', { type:'bar', data:{ labels:periodos.map(([p])=>p), datasets:[{ label:'Custo R$', data:periodos.map(([,v])=>Math.round(v)), backgroundColor:C.yellow, borderRadius:4 }] }, options:{ maintainAspectRatio:false, plugins:{ legend:{ display:false }, tooltip:{ enabled:false }}, scales:{ y:{ beginAtZero:true, ticks:{ callback:v=>'R$'+v.toLocaleString('pt-BR') }}}}});
  const topProd = Object.entries(custoPorProduto).sort((a,b)=>b[1].total-a[1].total).slice(0,15);
  makeChart('chart-custo-produto', { type:'bar', data:{ labels:topProd.map(([p])=>p), datasets:[{ label:'Custo Total R$', data:topProd.map(([,v])=>Math.round(v.total)), backgroundColor:C.orange, borderRadius:4 },{ label:'Custo Médio R$', data:topProd.map(([,v])=>Math.round(v.total/v.count)), backgroundColor:C.blue, borderRadius:4 }] }, options:{ maintainAspectRatio:false, indexAxis:'y', plugins:{ legend:{ position:'bottom', labels:{ boxWidth:12, color:C.white }}, tooltip:{ callbacks:{ label:ctx=>` ${ctx.dataset.label}: R$${Math.round(ctx.raw).toLocaleString('pt-BR')} (${topProd[ctx.dataIndex][1].count} tickets)` }}}, scales:{ x:{ beginAtZero:true, ticks:{ callback:v=>'R$'+v.toLocaleString('pt-BR') }}}}});
  const fmt = v => v.toLocaleString('pt-BR',{ minimumFractionDigits:2, maximumFractionDigits:2 });
  document.getElementById('tbody-custo-tickets').innerHTML = ticketsCusto.slice(0,200).map(t => {
    const gt=(t.garantia||'').toLowerCase().includes('no warranty maintenance')?'Manutenção Paga':(t.garantia||'').toLowerCase().includes('no warranty')?'Fora Garantia':'Garantia';
    const gc=gt==='Garantia'?C.yellow:gt==='Fora Garantia'?C.green:C.orange;
    const data=t.data?new Date(t.data).toLocaleDateString('pt-BR'):'—';
    return `<tr><td><strong>#${t.ticket}</strong></td><td style="max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="${t.produto}">${t.produto}</td><td>${t.fornecedor}</td><td style="color:${gc};font-weight:600;">${gt}</td><td class="num">${t.horasUteis.toFixed(1)}h</td><td class="num" style="color:var(--yellow);font-weight:600;">R$ ${fmt(t.custo)}</td><td style="font-size:10px;color:var(--gray2);">${t.statusList}</td><td>${data}</td></tr>`;
  }).join('') || '<tr><td colspan="8" style="text-align:center;color:var(--gray2);padding:20px;">Sem dados no período — aguarde o histórico carregar</td></tr>';
}

// ════════════════════════════════════════════════════════════════════
// ABA: IMPORTAÇÕES
// ════════════════════════════════════════════════════════════════════
function renderImportacoes(d) {
  const r = d.rma;
  document.getElementById('tbody-components').innerHTML = r.topComponents.map(c => `<tr><td style="font-family:'Barlow Condensed',sans-serif;">${c.sku}</td><td>${c.model||'—'}</td><td class="num">${c.count}</td><td class="num" style="color:var(--blue)">${c.warranty}</td><td class="num" style="color:var(--red)">${c.noWarranty}</td></tr>`).join('') || '<tr><td colspan="5" style="text-align:center;color:var(--gray2);">Sem dados de componentes</td></tr>';
  const months=Object.keys(r.monthlyConsumption).sort(), warrantyByMonth=months.map(m=>Object.values(r.monthlyConsumption[m]).reduce((s,v)=>s+v.warranty,0)), noWarrantyByMonth=months.map(m=>Object.values(r.monthlyConsumption[m]).reduce((s,v)=>s+v.noWarranty,0));
  makeChart('chart-monthly', { type:'bar', data:{ labels:months, datasets:[{ label:'Em Garantia', data:warrantyByMonth, backgroundColor:C.blue, borderRadius:3 },{ label:'Fora de Garantia', data:noWarrantyByMonth, backgroundColor:C.red, borderRadius:3 }] }, options:{ scales:{ x:{ stacked:true }, y:{ stacked:true, beginAtZero:true }}, plugins:{ legend:{ position:'bottom' }}}});
  renderSpareParts();
  if (d.sankhya.length > 0) {
    const keys = Object.keys(d.sankhya[0]);
    document.getElementById('thead-sankhya').innerHTML = keys.map(k=>`<th>${k}</th>`).join('');
    document.getElementById('tbody-sankhya').innerHTML = d.sankhya.slice(0,100).map(row=>`<tr>${keys.map(k=>`<td>${row[k]||'—'}</td>`).join('')}</tr>`).join('');
  } else { document.getElementById('tbody-sankhya').innerHTML = '<tr><td colspan="10" style="text-align:center;color:var(--gray2);">Faça upload do CSV do Sankhya na aba Dados</td></tr>'; }
}

let stockMins = JSON.parse(localStorage.getItem('stockMins')||'{}');
function renderSpareParts() {
  if (!state.report) return;
  const parts=state.report.spareParts, search=document.getElementById('search-parts')?.value.toLowerCase()||'';
  const filtered = parts.filter(p => p.modelo?.toLowerCase().includes(search)||p.sku?.toLowerCase().includes(search)||p.categoria?.toLowerCase().includes(search));
  document.getElementById('tbody-spare').innerHTML = filtered.map(p => {
    const min=stockMins[p.sku]||0, alert=p.totalFisico<=min&&min>0;
    const statusBadge = alert?`<span class="badge vencido">⚠️ Crítico</span>`:min>0?`<span class="badge ok">OK</span>`:`<span style="color:var(--gray2);font-size:11px;">—</span>`;
    return `<tr class="${alert?'alert-row':''}"><td>${p.fornecedor||''}</td><td>${p.categoria}</td><td style="font-family:'Barlow Condensed',sans-serif;">${p.sku}</td><td style="max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="${p.modelo}">${p.modelo}</td><td class="num">${p.quantidade}</td><td class="num">${p.saida}</td><td class="num" style="font-weight:600;">${p.totalFisico}</td><td class="num"><input class="stock-min-input" type="number" min="0" value="${min}" onchange="updateMin('${p.sku}',this.value)"/></td><td>${statusBadge}</td></tr>`;
  }).join('') || '<tr><td colspan="9" style="text-align:center;color:var(--gray2);">Faça upload do CSV de Spare Parts na aba Dados</td></tr>';
}
function updateMin(sku, val) { stockMins[sku] = parseInt(val)||0; }
async function saveStockMins() {
  localStorage.setItem('stockMins', JSON.stringify(stockMins));
  try { await fetch('/api/stock-min', { method:'POST', headers:{ 'Content-Type':'application/json' }, body:JSON.stringify(stockMins) }); } catch {}
  renderSpareParts(); alert('Estoque mínimo salvo!');
}

// ════════════════════════════════════════════════════════════════════
// ABA: DADOS
// ════════════════════════════════════════════════════════════════════
function handleFile(type, input) {
  const file = input.files[0]; if (!file) return;
  state.files[type] = file;
  const nameEl = document.getElementById(`fname-${type}`); if (nameEl) nameEl.textContent = file.name;
}
async function uploadFile(type) {
  const endpoints = { rma:'/api/upload/rma', spare:'/api/upload/spare-parts', sankhya:'/api/upload/sankhya' };
  const file = state.files[type], statusEl = document.getElementById(`status-${type}`);
  if (!file) { if (statusEl) { statusEl.style.display='block'; statusEl.className='upload-status err'; statusEl.textContent='Selecione um arquivo primeiro.'; } return; }
  const fd = new FormData(); fd.append('file', file);
  try {
    statusEl.style.display='block'; statusEl.className='upload-status'; statusEl.textContent='Enviando...';
    const res = await fetch(endpoints[type], { method:'POST', body:fd });
    const data = await res.json();
    if (data.ok) { statusEl.className='upload-status ok'; statusEl.textContent=`✅ ${data.count} registros carregados`; loadData(); }
    else { statusEl.className='upload-status err'; statusEl.textContent=`Erro: ${data.error}`; }
  } catch(e) { statusEl.className='upload-status err'; statusEl.textContent=`Erro: ${e.message}`; }
}
function renderSourceStatus(ds) {
  document.getElementById('source-status').innerHTML = [
    { key:'desk', label:'Zoho Desk', desc:'API automática — atualiza a cada 2h' },
    { key:'rma', label:'RMA Zoho Forms', desc:'Upload manual de CSV' },
    { key:'sankhya', label:'Sankhya Importações', desc:'Upload manual de CSV' },
    { key:'spareParts', label:'Spare Parts', desc:'Upload manual de CSV' }
  ].map(i => `<div style="display:flex;justify-content:space-between;align-items:center;padding:10px;background:var(--bg3);border-radius:4px;border:1px solid var(--border);"><div><div style="font-weight:600;font-size:13px;">${i.label}</div><div style="font-size:11px;color:var(--gray2);margin-top:2px;">${i.desc}</div></div><span class="ds-badge ${ds[i.key]?'ok':'missing'}">${ds[i.key]?'● Carregado':'○ Sem dados'}</span></div>`).join('');
}

// ── Drag and drop nas upload zones ───────────────────────────────────
['rma','spare','sankhya'].forEach(type => {
  const zone = document.getElementById(`zone-${type}`); if (!zone) return;
  zone.addEventListener('dragover', e => { e.preventDefault(); zone.classList.add('dragover'); });
  zone.addEventListener('dragleave', () => zone.classList.remove('dragover'));
  zone.addEventListener('drop', e => {
    e.preventDefault(); zone.classList.remove('dragover');
    const file = e.dataTransfer.files[0];
    if (file) { state.files[type]=file; const nameEl=document.getElementById(`fname-${type}`); if (nameEl) nameEl.textContent=file.name; }
  });
});

// ── Init ─────────────────────────────────────────────────────────────
setInterval(loadData, 2 * 60 * 60 * 1000);
setPeriodAll();
loadData();
