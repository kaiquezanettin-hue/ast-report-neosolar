const express = require('express');
const axios = require('axios');
const multer = require('multer');
const csv = require('csv-parse/sync');
const path = require('path');

const app = express();
app.use(express.json({ limit: '50mb' }));

const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 50 * 1024 * 1024 } });

// ─── Supabase helpers ────────────────────────────────────────────────
const SUPA_URL = process.env.SUPABASE_URL;
const SUPA_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

async function dbGet(key) {
  try {
    const res = await axios.get(`${SUPA_URL}/rest/v1/ast_storage?key=eq.${key}&select=value,updated_at`, {
      headers: { apikey: SUPA_KEY, Authorization: `Bearer ${SUPA_KEY}` }
    });
    if (res.data && res.data.length > 0) {
      return { value: JSON.parse(res.data[0].value), updated_at: res.data[0].updated_at };
    }
    return { value: null, updated_at: null };
  } catch { return { value: null, updated_at: null }; }
}

async function dbSet(key, value) {
  try {
    await axios.post(`${SUPA_URL}/rest/v1/ast_storage`, {
      key, value: JSON.stringify(value), updated_at: new Date().toISOString()
    }, {
      headers: {
        apikey: SUPA_KEY,
        Authorization: `Bearer ${SUPA_KEY}`,
        'Content-Type': 'application/json',
        Prefer: 'resolution=merge-duplicates'
      }
    });
    return true;
  } catch (e) { console.error('dbSet error:', e.message); return false; }
}

// ─── In-memory cache ─────────────────────────────────────────────────
let memCache = {
  desk: { data: null, ts: 0 },
  rma: { data: null, ts: 0, updated_at: null },
  sankhya: { data: null, ts: 0, updated_at: null },
  sheets: { data: null, ts: 0, updated_at: null },
  stockMin: { data: {}, ts: 0 }
};
const MEM_TTL = 10 * 60 * 1000;
const DESK_TTL = 2 * 60 * 60 * 1000;

// ─── Zoho Desk Auth ──────────────────────────────────────────────────
let deskToken = { access_token: null, expires_at: 0 };

async function getDeskToken() {
  if (deskToken.access_token && Date.now() < deskToken.expires_at) return deskToken.access_token;
  const res = await axios.post('https://accounts.zoho.com/oauth/v2/token', null, {
    params: {
      refresh_token: process.env.ZOHO_REFRESH_TOKEN,
      client_id: process.env.ZOHO_CLIENT_ID,
      client_secret: process.env.ZOHO_CLIENT_SECRET,
      grant_type: 'refresh_token'
    }
  });
  deskToken.access_token = res.data.access_token;
  deskToken.expires_at = Date.now() + (res.data.expires_in - 60) * 1000;
  return deskToken.access_token;
}

async function fetchDeskTickets() {
  if (memCache.desk.data && Date.now() - memCache.desk.ts < DESK_TTL) return memCache.desk.data;

  const token = await getDeskToken();
  const deptId = process.env.ZOHO_DEPT_ID;
  let all = [], from = 0;
  const limit = 100;

  while (true) {
    await new Promise(r => setTimeout(r, 150));
    const res = await axios.get('https://desk.zoho.com/api/v1/tickets', {
      headers: { Authorization: `Zoho-oauthtoken ${token}` },
      params: { departmentId: deptId, limit, from, status: 'open', include: 'assignee' }
    });
    const tickets = res.data.data || [];
    all = all.concat(tickets);
    if (tickets.length < limit) break;
    from += limit;
  }

  const enriched = [];
  for (const t of all) {
    await new Promise(r => setTimeout(r, 150));
    try {
      const hist = await axios.get(`https://desk.zoho.com/api/v1/tickets/${t.id}/History`, {
        headers: { Authorization: `Zoho-oauthtoken ${token}` }
      });
      const history = hist.data.data || [];
      const currentStatusEntry = [...history].reverse().find(h => h.fieldName === 'Status' && h.to === t.status);
      const statusSince = currentStatusEntry ? new Date(currentStatusEntry.modifiedTime) : new Date(t.createdTime);
      const hoursInStatus = (Date.now() - statusSince.getTime()) / 3600000;
      enriched.push({ ...t, statusSince: statusSince.toISOString(), hoursInStatus });
    } catch {
      enriched.push({ ...t, statusSince: t.createdTime, hoursInStatus: 0 });
    }
  }

  memCache.desk = { data: enriched, ts: Date.now() };
  return enriched;
}

const SLA = {
  'Aguardando Teste': 72, 'Em Teste': 6, 'Em Manutenção': 24,
  'Aguardando Peça Reposição': 1080, 'Ag. Aprovação Manutenção SG': 48,
  'Em Tratativa Devolução Cliente': 48, 'Aguardando Manutenção SG': 24, 'Aguardando Laudo': 3
};

function getSlaStatus(status, hoursInStatus) {
  const sla = SLA[status];
  if (!sla) return 'ok';
  const pct = hoursInStatus / sla;
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
    serial: r['Serial #'] || '',
    invoiceDate: r['Date of Sale to End Customer'] || '',
    fault: r['Fault description'] || '',
    testDate: r['Test Date in the lab'] || '',
    validation: r['Validation'] || '',
    service: r['Service Performed'] || '',
    skuComponents: r['SKU dos componentes consumidos no reparo'] || '',
    componentModel: r['Model PCB / Component'] || '',
    businessUnit: r['Business Unit'] || '',
    addedTime: r['Added Time'] || '',
    taskOwner: r['Task Owner'] || ''
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

// ─── Load from Supabase with mem cache ──────────────────────────────
async function getFromDb(key, memKey) {
  if (memCache[memKey].data && Date.now() - memCache[memKey].ts < MEM_TTL) {
    return memCache[memKey];
  }
  const row = await dbGet(key);
  if (row.value) {
    memCache[memKey] = { data: row.value, ts: Date.now(), updated_at: row.updated_at };
  }
  return memCache[memKey];
}

// ─── Routes ──────────────────────────────────────────────────────────
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

app.get('/api/report', async (req, res) => {
  try {
    const { from, to } = req.query;
    const fromDate = from ? new Date(from) : new Date(0);
    const toDate = to ? new Date(to) : new Date();

    // Desk
    let deskTickets = [], deskUpdatedAt = null;
    try {
      deskTickets = await fetchDeskTickets();
      deskUpdatedAt = new Date(memCache.desk.ts).toISOString();
    } catch (e) { console.error('Desk error:', e.message); }

    // RMA
    const rmaCache = await getFromDb('rma', 'rma');
    let rma = (rmaCache.data || []).filter(r => {
      const d = new Date(r.testDate);
      return d >= fromDate && d <= toDate;
    });

    // Sankhya
    const sankhyaCache = await getFromDb('sankhya', 'sankhya');
    const sankhya = sankhyaCache.data || [];

    // Spare parts + stock min
    const sheetsCache = await getFromDb('spare_parts', 'sheets');
    const stockMinCache = await getFromDb('stock_min', 'stockMin');
    const stockMins = stockMinCache.data || {};
    const spareParts = (sheetsCache.data || []).map(p => ({
      ...p,
      minStock: stockMins[p.sku] || 0,
      alert: p.quantidade <= (stockMins[p.sku] || 0) && (stockMins[p.sku] || 0) > 0
    }));

    // Desk SLA
    const slaByStatus = {}, technicians = {};
    for (const t of deskTickets) {
      const status = t.status;
      const agent = t.assignee?.name || 'Sem agente';
      const slaS = getSlaStatus(status, t.hoursInStatus || 0);
      if (!slaByStatus[status]) slaByStatus[status] = { total: 0, ok: 0, atencao: 0, vencido: 0, totalHours: 0 };
      slaByStatus[status].total++;
      slaByStatus[status][slaS]++;
      slaByStatus[status].totalHours += t.hoursInStatus || 0;
      if (!technicians[agent]) technicians[agent] = { total: 0, ok: 0, atencao: 0, vencido: 0 };
      technicians[agent].total++;
      technicians[agent][slaS]++;
    }

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
      period: { from: fromDate.toISOString(), to: toDate.toISOString() },
      desk: { total: deskTickets.length, slaByStatus, byTechnician: technicians, tickets: deskTickets.slice(0, 200), updatedAt: deskUpdatedAt },
      rma: { total: rma.length, topProducts, topFaults, lineCount, serviceCount, warrantyCount, topComponents, monthlyConsumption },
      spareParts,
      sankhya: sankhya.slice(0, 500),
      dataStatus: {
        desk: deskTickets.length > 0,
        rma: (rmaCache.data || []).length > 0,
        sankhya: sankhya.length > 0,
        spareParts: spareParts.length > 0
      },
      lastUpdated: {
        desk: deskUpdatedAt,
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

app.get('/api/status', async (req, res) => {
  const rma = await getFromDb('rma', 'rma');
  const sankhya = await getFromDb('sankhya', 'sankhya');
  const sheets = await getFromDb('spare_parts', 'sheets');
  res.json({
    desk: { loaded: !!memCache.desk.data, count: memCache.desk.data?.length || 0, updatedAt: memCache.desk.ts ? new Date(memCache.desk.ts).toISOString() : null },
    rma: { loaded: !!(rma.data), count: (rma.data || []).length, updatedAt: rma.updated_at || null },
    sankhya: { loaded: !!(sankhya.data), count: (sankhya.data || []).length, updatedAt: sankhya.updated_at || null },
    sheets: { loaded: !!(sheets.data), count: (sheets.data || []).length, updatedAt: sheets.updated_at || null }
  });
});

app.get('/api/debug-desk', async (req, res) => {
  try {
    const token = await getDeskToken();
    const r1 = await axios.get('https://desk.zoho.com/api/v1/departments', {
      headers: { Authorization: `Zoho-oauthtoken ${token}` }
    });
    res.json({ departments: r1.data });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.use(express.static(path.join(__dirname, '../public')));

module.exports = app;
