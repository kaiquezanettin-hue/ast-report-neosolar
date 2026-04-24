const express = require('express');
const axios = require('axios');
const multer = require('multer');
const csv = require('csv-parse/sync');
const path = require('path');

const app = express();
app.use(express.json({ limit: '50mb' }));
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 50 * 1024 * 1024 } });

// ─── Supabase ────────────────────────────────────────────────────────
const SUPA_URL = process.env.SUPABASE_URL;
const SUPA_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

async function dbGet(key) {
  try {
    const res = await axios.get(
      `${SUPA_URL}/rest/v1/ast_storage?key=eq.${encodeURIComponent(key)}&select=value,updated_at`,
      { headers: { apikey: SUPA_KEY, Authorization: `Bearer ${SUPA_KEY}` } }
    );
    if (res.data && res.data.length > 0) {
      return { value: JSON.parse(res.data[0].value), updated_at: res.data[0].updated_at };
    }
    return { value: null, updated_at: null };
  } catch { return { value: null, updated_at: null }; }
}

async function dbSet(key, value) {
  try {
    await axios.post(`${SUPA_URL}/rest/v1/ast_storage`,
      { key, value: JSON.stringify(value), updated_at: new Date().toISOString() },
      {
        headers: {
          apikey: SUPA_KEY, Authorization: `Bearer ${SUPA_KEY}`,
          'Content-Type': 'application/json', Prefer: 'resolution=merge-duplicates'
        }
      }
    );
    return true;
  } catch (e) { console.error('dbSet error:', e.message); return false; }
}

// ─── Memory cache ────────────────────────────────────────────────────
let memCache = {
  deskOpen: { data: null, ts: 0, updated_at: null, loading: false },
  deskClosed: { data: null, ts: 0, updated_at: null, loading: false },
  rma: { data: null, ts: 0, updated_at: null },
  sankhya: { data: null, ts: 0, updated_at: null },
  sheets: { data: null, ts: 0, updated_at: null },
  stockMin: { data: {}, ts: 0 }
};
const MEM_TTL = 10 * 60 * 1000;
const DESK_OPEN_TTL = 2 * 60 * 60 * 1000;   // 2h para abertos
const DESK_CLOSED_TTL = 12 * 60 * 60 * 1000; // 12h para fechados
const delay = ms => new Promise(r => setTimeout(r, ms));

// ─── Zoho Auth ───────────────────────────────────────────────────────
let zohoToken = { access_token: null, expires_at: 0 };

async function getDeskToken() {
  if (zohoToken.access_token && Date.now() < zohoToken.expires_at) return zohoToken.access_token;
  const res = await axios.post('https://accounts.zoho.com/oauth/v2/token', null, {
    params: {
      refresh_token: process.env.ZOHO_REFRESH_TOKEN,
      client_id: process.env.ZOHO_CLIENT_ID,
      client_secret: process.env.ZOHO_CLIENT_SECRET,
      grant_type: 'refresh_token'
    }
  });
  zohoToken.access_token = res.data.access_token;
  zohoToken.expires_at = Date.now() + (res.data.expires_in - 60) * 1000;
  return zohoToken.access_token;
}

// ─── Fetch OPEN tickets (rápido, sem histórico) ───────────────────────
async function fetchOpenTickets() {
  if (memCache.deskOpen.data && Date.now() - memCache.deskOpen.ts < DESK_OPEN_TTL) {
    return memCache.deskOpen.data;
  }
  if (memCache.deskOpen.loading) {
    await delay(300);
    return memCache.deskOpen.data || [];
  }
  memCache.deskOpen.loading = true;
  try {
    const token = await getDeskToken();
    const deptId = process.env.ZOHO_DEPT_ID;
    let all = [], from = 0;
    while (true) {
      await delay(150);
      const res = await axios.get('https://desk.zoho.com/api/v1/tickets', {
        headers: { Authorization: `Zoho-oauthtoken ${token}` },
        params: { departmentId: deptId, limit: 100, from, status: 'open', include: 'assignee' }
      });
      const tickets = res.data.data || [];
      all = all.concat(tickets);
      if (tickets.length < 100) break;
      from += 100;
    }

    const enriched = all.map(t => ({
      id: t.id,
      ticketNumber: t.ticketNumber,
      subject: t.subject,
      status: t.status,
      assigneeName: t.assignee?.name || 'Sem agente',
      createdTime: t.createdTime,
      closedTime: null,
      hoursInCurrentStatus: (Date.now() - new Date(t.modifiedTime || t.createdTime).getTime()) / 3600000,
      totalHours: (Date.now() - new Date(t.createdTime).getTime()) / 3600000
    }));

    memCache.deskOpen = { data: enriched, ts: Date.now(), updated_at: new Date().toISOString(), loading: false };
    console.log(`Open tickets carregados: ${enriched.length}`);
    return enriched;
  } catch (e) {
    memCache.deskOpen.loading = false;
    console.error('fetchOpenTickets error:', e.message);
    return memCache.deskOpen.data || [];
  }
}

// ─── Fetch CLOSED tickets (com histórico de status) ───────────────────
async function fetchClosedTickets() {
  if (memCache.deskClosed.data && Date.now() - memCache.deskClosed.ts < DESK_CLOSED_TTL) {
    return memCache.deskClosed.data;
  }
  if (memCache.deskClosed.loading) {
    await delay(300);
    return memCache.deskClosed.data || [];
  }
  memCache.deskClosed.loading = true;
  try {
    const token = await getDeskToken();
    const deptId = process.env.ZOHO_DEPT_ID;
    let all = [], from = 0;

    // Busca tickets fechados
    while (true) {
      await delay(150);
      const res = await axios.get('https://desk.zoho.com/api/v1/tickets', {
        headers: { Authorization: `Zoho-oauthtoken ${token}` },
        params: { departmentId: deptId, limit: 100, from, status: 'closed', include: 'assignee' }
      });
      const tickets = res.data.data || [];
      all = all.concat(tickets);
      if (tickets.length < 100) break;
      from += 100;
    }

    // Histórico de status para calcular tempo em cada status
    const enriched = [];
    for (const t of all) {
      await delay(150);
      try {
        const hist = await axios.get(`https://desk.zoho.com/api/v1/tickets/${t.id}/History`, {
          headers: { Authorization: `Zoho-oauthtoken ${token}` }
        });
        const history = (hist.data.data || []).filter(h => h.fieldName === 'Status');
        const sorted = [...history].sort((a, b) => new Date(a.modifiedTime) - new Date(b.modifiedTime));

        const statusTimes = {};
        for (let i = 0; i < sorted.length; i++) {
          const sName = sorted[i].to;
          const start = new Date(sorted[i].modifiedTime);
          const end = i < sorted.length - 1 ? new Date(sorted[i + 1].modifiedTime) : new Date(t.closedTime || Date.now());
          const hours = (end - start) / 3600000;
          if (hours > 0 && hours < 8760) statusTimes[sName] = (statusTimes[sName] || 0) + hours;
        }

        enriched.push({
          id: t.id,
          ticketNumber: t.ticketNumber,
          subject: t.subject,
          status: t.status,
          assigneeName: t.assignee?.name || 'Sem agente',
          createdTime: t.createdTime,
          closedTime: t.closedTime || null,
          hoursInCurrentStatus: 0,
          statusTimes,
          totalHours: t.closedTime ? (new Date(t.closedTime) - new Date(t.createdTime)) / 3600000 : 0
        });
      } catch {
        enriched.push({
          id: t.id, ticketNumber: t.ticketNumber, subject: t.subject,
          status: t.status, assigneeName: t.assignee?.name || 'Sem agente',
          createdTime: t.createdTime, closedTime: t.closedTime || null,
          hoursInCurrentStatus: 0, statusTimes: {}, totalHours: 0
        });
      }
    }

    memCache.deskClosed = { data: enriched, ts: Date.now(), updated_at: new Date().toISOString(), loading: false };
    console.log(`Closed tickets carregados: ${enriched.length}`);
    return enriched;
  } catch (e) {
    memCache.deskClosed.loading = false;
    console.error('fetchClosedTickets error:', e.message);
    return memCache.deskClosed.data || [];
  }
}

// Pre-fetch ao iniciar servidor
setTimeout(() => {
  fetchOpenTickets().catch(console.error);
  // Fechados em background após os abertos
  setTimeout(() => fetchClosedTickets().catch(console.error), 30000);
}, 2000);

// ─── SLA map ─────────────────────────────────────────────────────────
const SLA = {
  'Aguardando Teste': 72, 'Ag. teste': 72,
  'Em Teste': 6, 'Em teste': 6,
  'Em Manutenção': 24, 'Em manutenção': 24,
  'Aguardando Peça Reposição': 1080, 'Ag. Peça Reposição': 1080,
  'Ag. Aprovação Manutenção SG': 48, 'Ag. aprovação manutenção (SG)': 48,
  'Em Tratativa Devolução Cliente': 48, 'Em tratativa p/ devolução cliente': 48,
  'Aguardando Manutenção SG': 24, 'Aguardando Laudo': 3
};

function getSlaStatus(status, hours) {
  const sla = SLA[status];
  if (!sla) return 'ok';
  const pct = hours / sla;
  if (pct >= 1) return 'vencido';
  if (pct >= 0.75) return 'atencao';
  return 'ok';
}

// ─── CSV Parsers ─────────────────────────────────────────────────────
function parseRmaCsv(buffer) {
  const records = csv.parse(buffer, { columns: true, skip_empty_lines: true, trim: true });
  return records.map(r => ({
    fornecedor: r['Fornecedor'] || '',
    deskNum: r['Desk #'] || '',
    model: r['Model #'] || '',
    sku: r['SKU'] || '',
    fault: r['Fault description'] || '',
    testDate: r['Test Date in the lab'] || '',
    validation: r['Validation'] || '',
    service: r['Service Performed'] || '',
    skuComponents: r['SKU dos componentes consumidos no reparo'] || '',
    componentModel: r['Model PCB / Component'] || '',
    businessUnit: r['Business Unit'] || '',
    addedTime: r['Added Time'] || ''
  }));
}

function parseSankhyaCsv(buffer) {
  return csv.parse(buffer, { columns: true, skip_empty_lines: true, trim: true });
}

function parseSheetsCsv(buffer) {
  const records = csv.parse(buffer, { columns: true, skip_empty_lines: true, trim: true });
  return records.map(r => ({
    categoria: r['Categoria'] || '',
    sku: r['SKU'] || '',
    modelo: r['Modelo'] || '',
    quantidade: parseInt(r['Quantidade']) || 0,
    saida: parseInt(r['Saida']) || 0,
    totalFisico: parseInt(r['Total Fisico']) || 0
  }));
}

// ─── Supabase cache loader ────────────────────────────────────────────
async function getFromDb(key, memKey) {
  if (memCache[memKey].data && Date.now() - memCache[memKey].ts < MEM_TTL) return memCache[memKey];
  const row = await dbGet(key);
  if (row.value) {
    const actualData = row.value && row.value.data !== undefined ? row.value.data : row.value;
    const actualUpdatedAt = (row.value && row.value.updated_at) ? row.value.updated_at : row.updated_at;
    memCache[memKey] = { data: actualData, ts: Date.now(), updated_at: actualUpdatedAt };
  }
  return memCache[memKey];
}

// ─── Upload routes ────────────────────────────────────────────────────
app.post('/api/upload/rma', upload.single('file'), async (req, res) => {
  try {
    const data = parseRmaCsv(req.file.buffer);
    const updated_at = new Date().toISOString();
    await dbSet('rma', { data, updated_at });
    memCache.rma = { data, ts: Date.now(), updated_at };
    res.json({ ok: true, count: data.length, updated_at });
  } catch (e) { res.status(400).json({ error: e.message }); }
});

app.post('/api/upload/sankhya', upload.single('file'), async (req, res) => {
  try {
    const data = parseSankhyaCsv(req.file.buffer);
    const updated_at = new Date().toISOString();
    await dbSet('sankhya', { data, updated_at });
    memCache.sankhya = { data, ts: Date.now(), updated_at };
    res.json({ ok: true, count: data.length, updated_at });
  } catch (e) { res.status(400).json({ error: e.message }); }
});

app.post('/api/upload/spare-parts', upload.single('file'), async (req, res) => {
  try {
    const data = parseSheetsCsv(req.file.buffer);
    const updated_at = new Date().toISOString();
    await dbSet('spare_parts', { data, updated_at });
    memCache.sheets = { data, ts: Date.now(), updated_at };
    res.json({ ok: true, count: data.length, updated_at });
  } catch (e) { res.status(400).json({ error: e.message }); }
});

app.post('/api/stock-min', async (req, res) => {
  const current = (await dbGet('stock_min')).value || {};
  const merged = { ...current, ...req.body };
  await dbSet('stock_min', merged);
  memCache.stockMin = { data: merged, ts: Date.now() };
  res.json({ ok: true });
});

// ─── Report endpoint ──────────────────────────────────────────────────
app.get('/api/report', async (req, res) => {
  try {
    const { from, to } = req.query;
    const fromDate = from ? new Date(from) : new Date(0);
    const toDate = to ? new Date(to) : new Date();

    // Tickets abertos — rápido
    const openTickets = await fetchOpenTickets();

    // Tickets fechados — background (retorna o que tiver em cache)
    const closedTickets = memCache.deskClosed.data || [];
    const closedLoading = memCache.deskClosed.loading;

    // Todos os tickets para análise do período
    const allTickets = [...openTickets, ...closedTickets];
    const periodTickets = allTickets.filter(t => {
      const d = new Date(t.createdTime);
      return d >= fromDate && d <= toDate;
    });

    // SLA atual — apenas abertos
    const slaByStatus = {}, technicians = {};
    for (const t of openTickets) {
      const slaS = getSlaStatus(t.status, t.hoursInCurrentStatus || 0);
      if (!slaByStatus[t.status]) slaByStatus[t.status] = { total: 0, ok: 0, atencao: 0, vencido: 0, totalHours: 0 };
      slaByStatus[t.status].total++;
      slaByStatus[t.status][slaS]++;
      slaByStatus[t.status].totalHours += t.hoursInCurrentStatus || 0;
      if (!technicians[t.assigneeName]) technicians[t.assigneeName] = { total: 0, ok: 0, atencao: 0, vencido: 0 };
      technicians[t.assigneeName].total++;
      technicians[t.assigneeName][slaS]++;
    }

    // Tempo médio histórico por status — tickets fechados do período
    const closedPeriod = closedTickets.filter(t => {
      const d = new Date(t.createdTime);
      return d >= fromDate && d <= toDate;
    });
    const avgTimeByStatus = {};
    for (const t of closedPeriod) {
      for (const [status, hours] of Object.entries(t.statusTimes || {})) {
        if (!avgTimeByStatus[status]) avgTimeByStatus[status] = { totalHours: 0, count: 0 };
        avgTimeByStatus[status].totalHours += hours;
        avgTimeByStatus[status].count++;
      }
    }
    const avgTimeByStatusResult = Object.entries(avgTimeByStatus)
      .map(([status, v]) => ({ status, avgHours: v.count > 0 ? v.totalHours / v.count : 0, count: v.count, sla: SLA[status] || null }))
      .filter(a => a.avgHours > 0)
      .sort((a, b) => b.avgHours - a.avgHours);

    // Performance por técnico — fechados do período
    const avgTimeByAgent = {};
    for (const t of closedPeriod) {
      if (!avgTimeByAgent[t.assigneeName]) avgTimeByAgent[t.assigneeName] = { totalHours: 0, count: 0, closed: 0 };
      avgTimeByAgent[t.assigneeName].totalHours += t.totalHours || 0;
      avgTimeByAgent[t.assigneeName].count++;
      avgTimeByAgent[t.assigneeName].closed++;
    }

    // Tendência mensal
    const monthlyTrend = {};
    for (const t of periodTickets) {
      const month = t.createdTime?.substring(0, 7) || 'N/A';
      if (!monthlyTrend[month]) monthlyTrend[month] = { opened: 0, closed: 0 };
      monthlyTrend[month].opened++;
      if (t.closedTime) monthlyTrend[month].closed++;
    }

    // RMA do Supabase
    const rmaCache = await getFromDb('rma', 'rma');
    const rma = (rmaCache.data || []).filter(r => {
      const d = new Date(r.testDate);
      return d >= fromDate && d <= toDate;
    });

    // Spare Parts + Sankhya
    const sankhyaCache = await getFromDb('sankhya', 'sankhya');
    const sheetsCache = await getFromDb('spare_parts', 'sheets');
    const stockMinCache = await getFromDb('stock_min', 'stockMin');
    const stockMins = stockMinCache.data || {};
    const spareParts = (sheetsCache.data || []).map(p => ({
      ...p, minStock: stockMins[p.sku] || 0,
      alert: p.quantidade <= (stockMins[p.sku] || 0) && (stockMins[p.sku] || 0) > 0
    }));

    // RMA processing
    const productCount = {}, faultCount = {}, lineCount = {}, serviceCount = {};
    const warrantyCount = { warranty: 0, noWarranty: 0, noWarrantyMaint: 0 };
    const componentConsumption = {}, monthlyConsumption = {};

    for (const r of rma) {
      const modelKey = r.model || 'Desconhecido';
      if (!productCount[modelKey]) productCount[modelKey] = { count: 0, sku: r.sku, fornecedor: r.fornecedor };
      productCount[modelKey].count++;
      const faultCat = r.fault.split(' - ')[0] || r.fault;
      faultCount[faultCat] = (faultCount[faultCat] || 0) + 1;
      lineCount[r.fornecedor] = (lineCount[r.fornecedor] || 0) + 1;
      serviceCount[r.service] = (serviceCount[r.service] || 0) + 1;
      const v = r.validation.toLowerCase();
      if (v.includes('no warranty maintenance')) warrantyCount.noWarrantyMaint++;
      else if (v.includes('no warranty')) warrantyCount.noWarranty++;
      else if (v.includes('warranty')) warrantyCount.warranty++;

      if (r.skuComponents && r.skuComponents !== 'Sem SKU') {
        const skus = r.skuComponents.split(',').map(s => s.trim()).filter(Boolean);
        for (const sku of skus) {
          if (!componentConsumption[sku]) componentConsumption[sku] = { count: 0, warranty: 0, noWarranty: 0, model: r.componentModel };
          componentConsumption[sku].count++;
          if (v.includes('no warranty')) componentConsumption[sku].noWarranty++;
          else componentConsumption[sku].warranty++;
        }
        const month = r.testDate ? r.testDate.substring(3, 10) : 'N/A';
        if (!monthlyConsumption[month]) monthlyConsumption[month] = {};
        for (const sku of skus) {
          if (!monthlyConsumption[month][sku]) monthlyConsumption[month][sku] = { total: 0, warranty: 0, noWarranty: 0 };
          monthlyConsumption[month][sku].total++;
          if (v.includes('no warranty')) monthlyConsumption[month][sku].noWarranty++;
          else monthlyConsumption[month][sku].warranty++;
        }
      }
    }

    const topProducts = Object.entries(productCount).sort((a, b) => b[1].count - a[1].count).slice(0, 10).map(([model, d]) => ({ model, ...d }));
    const topFaults = Object.entries(faultCount).sort((a, b) => b[1] - a[1]).slice(0, 10).map(([fault, count]) => ({ fault, count }));
    const topComponents = Object.entries(componentConsumption).sort((a, b) => b[1].count - a[1].count).slice(0, 20).map(([sku, d]) => ({ sku, ...d }));

    res.json({
      updatedAt: new Date().toISOString(),
      deskOpenLoading: memCache.deskOpen.loading,
      deskClosedLoading: closedLoading,
      period: { from: fromDate.toISOString(), to: toDate.toISOString() },
      desk: {
        total: openTickets.length,
        totalClosed: closedTickets.length,
        totalHistorico: periodTickets.length,
        closedLoading,
        slaByStatus,
        byTechnician: technicians,
        avgTimeByStatus: avgTimeByStatusResult,
        avgTimeByAgent: Object.entries(avgTimeByAgent).map(([name, v]) => ({
          name, avgHours: v.count > 0 ? v.totalHours / v.count : 0, count: v.count, closed: v.closed
        })).sort((a, b) => b.count - a.count),
        monthlyTrend,
        updatedAt: memCache.deskOpen.updated_at
      },
      rma: { total: rma.length, topProducts, topFaults, lineCount, serviceCount, warrantyCount, topComponents, monthlyConsumption },
      spareParts,
      sankhya: (sankhyaCache.data || []).slice(0, 500),
      dataStatus: {
        desk: openTickets.length > 0,
        rma: (rmaCache.data || []).length > 0,
        sankhya: (sankhyaCache.data || []).length > 0,
        spareParts: spareParts.length > 0
      },
      lastUpdated: {
        desk: memCache.deskOpen.updated_at,
        deskClosed: memCache.deskClosed.updated_at,
        rma: rmaCache.updated_at || null,
        sankhya: sankhyaCache.updated_at || null,
        spareParts: sheetsCache.updated_at || null
      }
    });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

// ─── Status ───────────────────────────────────────────────────────────
app.get('/api/status', async (req, res) => {
  const rma = await getFromDb('rma', 'rma');
  const sankhya = await getFromDb('sankhya', 'sankhya');
  const sheets = await getFromDb('spare_parts', 'sheets');
  res.json({
    deskOpen: { loaded: !!memCache.deskOpen.data, loading: memCache.deskOpen.loading, count: (memCache.deskOpen.data || []).length, updatedAt: memCache.deskOpen.updated_at },
    deskClosed: { loaded: !!memCache.deskClosed.data, loading: memCache.deskClosed.loading, count: (memCache.deskClosed.data || []).length, updatedAt: memCache.deskClosed.updated_at },
    rma: { loaded: !!(rma.data), count: (rma.data || []).length, updatedAt: rma.updated_at },
    sankhya: { loaded: !!(sankhya.data), count: (sankhya.data || []).length, updatedAt: sankhya.updated_at },
    sheets: { loaded: !!(sheets.data), count: (sheets.data || []).length, updatedAt: sheets.updated_at }
  });
});

app.use(express.static(path.join(__dirname, '../public')));
module.exports = app;
