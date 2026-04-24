const express = require('express');
const axios = require('axios');
const multer = require('multer');
const csv = require('csv-parse/sync');
const path = require('path');

const app = express();
app.use(express.json({ limit: '50mb' }));

const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 50 * 1024 * 1024 } });

// ─── In-memory cache ────────────────────────────────────────────────
let cache = {
  desk: { data: null, ts: 0 },
  rma: { data: null, ts: 0 },
  sankhya: { data: null, ts: 0 },
  sheets: { data: null, ts: 0 },
  stockMin: {}
};
const CACHE_TTL = 2 * 60 * 60 * 1000; // 2h

// ─── Zoho Desk Auth ─────────────────────────────────────────────────
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

// ─── Zoho Desk: fetch all tickets ───────────────────────────────────
async function fetchDeskTickets() {
  if (cache.desk.data && Date.now() - cache.desk.ts < CACHE_TTL) return cache.desk.data;

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

  // Fetch history for SLA calculation
  const enriched = [];
  for (const t of all) {
    await new Promise(r => setTimeout(r, 150));
    try {
      const hist = await axios.get(`https://desk.zoho.com/api/v1/tickets/${t.id}/History`, {
        headers: { Authorization: `Zoho-oauthtoken ${token}` }
      });
      const history = hist.data.data || [];
      // Find when current status started
      const currentStatusEntry = [...history].reverse().find(h => h.fieldName === 'Status' && h.to === t.status);
      const statusSince = currentStatusEntry ? new Date(currentStatusEntry.modifiedTime) : new Date(t.createdTime);
      const hoursInStatus = (Date.now() - statusSince.getTime()) / 3600000;
      enriched.push({ ...t, statusSince: statusSince.toISOString(), hoursInStatus });
    } catch {
      enriched.push({ ...t, statusSince: t.createdTime, hoursInStatus: 0 });
    }
  }

  cache.desk = { data: enriched, ts: Date.now() };
  return enriched;
}

// SLA map (in hours)
const SLA = {
  'Aguardando Teste': 72,
  'Em Teste': 6,
  'Em Manutenção': 24,
  'Aguardando Peça Reposição': 1080,
  'Ag. Aprovação Manutenção SG': 48,
  'Em Tratativa Devolução Cliente': 48,
  'Aguardando Manutenção SG': 24,
  'Aguardando Laudo': 3
};

function getSlaStatus(status, hoursInStatus) {
  const sla = SLA[status];
  if (!sla) return 'ok';
  const pct = hoursInStatus / sla;
  if (pct >= 1) return 'vencido';
  if (pct >= 0.75) return 'atencao';
  return 'ok';
}

// ─── Parse RMA CSV ───────────────────────────────────────────────────
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

// ─── Parse Sankhya CSV ───────────────────────────────────────────────
function parseSankhyaCsv(buffer) {
  const records = csv.parse(buffer, { columns: true, skip_empty_lines: true, trim: true });
  return records;
}

// ─── Parse Sheets/Spare Parts CSV ───────────────────────────────────
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

// ─── API Routes ──────────────────────────────────────────────────────

// Upload RMA CSV
app.post('/api/upload/rma', upload.single('file'), (req, res) => {
  try {
    const data = parseRmaCsv(req.file.buffer);
    cache.rma = { data, ts: Date.now() };
    res.json({ ok: true, count: data.length });
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

// Upload Sankhya CSV
app.post('/api/upload/sankhya', upload.single('file'), (req, res) => {
  try {
    const data = parseSankhyaCsv(req.file.buffer);
    cache.sankhya = { data, ts: Date.now() };
    res.json({ ok: true, count: data.length });
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

// Upload Spare Parts CSV
app.post('/api/upload/spare-parts', upload.single('file'), (req, res) => {
  try {
    const data = parseSheetsCsv(req.file.buffer);
    cache.sheets = { data, ts: Date.now() };
    res.json({ ok: true, count: data.length });
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

// Save stock minimums
app.post('/api/stock-min', (req, res) => {
  cache.stockMin = { ...cache.stockMin, ...req.body };
  res.json({ ok: true });
});

// Main data endpoint
app.get('/api/report', async (req, res) => {
  try {
    const { from, to } = req.query;
    const fromDate = from ? new Date(from) : new Date(0);
    const toDate = to ? new Date(to) : new Date();

    // ── Desk data ──
    let deskTickets = [];
    try { deskTickets = await fetchDeskTickets(); } catch (e) { console.error('Desk error:', e.message); }

    // ── RMA data filtered by period ──
    let rma = cache.rma.data || [];
    rma = rma.filter(r => {
      const d = new Date(r.testDate);
      return d >= fromDate && d <= toDate;
    });

    // ── Sankhya data ──
    const sankhya = cache.sankhya.data || [];

    // ── Spare parts ──
    const spareParts = (cache.sheets.data || []).map(p => ({
      ...p,
      minStock: cache.stockMin[p.sku] || 0,
      alert: p.quantidade <= (cache.stockMin[p.sku] || 0)
    }));

    // ── Process Desk: SLA stats ──
    const slaByStatus = {};
    const slaByLine = {};
    const technicians = {};

    for (const t of deskTickets) {
      const status = t.status;
      const agent = t.assignee?.name || 'Sem agente';
      const slaS = getSlaStatus(status, t.hoursInStatus || 0);

      if (!slaByStatus[status]) slaByStatus[status] = { total: 0, ok: 0, atencao: 0, vencido: 0, totalHours: 0 };
      slaByStatus[status].total++;
      slaByStatus[status][slaS]++;
      slaByStatus[status].totalHours += t.hoursInStatus || 0;

      // Agent stats
      if (!technicians[agent]) technicians[agent] = { total: 0, ok: 0, atencao: 0, vencido: 0 };
      technicians[agent].total++;
      technicians[agent][slaS]++;
    }

    // ── Process RMA: Products ──
    const productCount = {};
    const faultCount = {};
    const lineCount = {};
    const serviceCount = {};
    const warrantyCount = { warranty: 0, noWarranty: 0, noWarrantyMaint: 0 };
    const componentConsumption = {};
    const monthlyConsumption = {};

    for (const r of rma) {
      // Products
      const modelKey = r.model || 'Desconhecido';
      if (!productCount[modelKey]) productCount[modelKey] = { count: 0, sku: r.sku, fornecedor: r.fornecedor };
      productCount[modelKey].count++;

      // Faults
      const faultCat = r.fault.split(' - ')[0] || r.fault;
      faultCount[faultCat] = (faultCount[faultCat] || 0) + 1;

      // Line (fornecedor)
      lineCount[r.fornecedor] = (lineCount[r.fornecedor] || 0) + 1;

      // Service
      serviceCount[r.service] = (serviceCount[r.service] || 0) + 1;

      // Warranty
      const v = r.validation.toLowerCase();
      if (v.includes('no warranty maintenance')) warrantyCount.noWarrantyMaint++;
      else if (v.includes('no warranty')) warrantyCount.noWarranty++;
      else if (v.includes('warranty')) warrantyCount.warranty++;

      // Component consumption
      if (r.skuComponents && r.skuComponents !== 'Sem SKU') {
        const skus = r.skuComponents.split(',').map(s => s.trim()).filter(Boolean);
        for (const sku of skus) {
          if (!componentConsumption[sku]) componentConsumption[sku] = { count: 0, warranty: 0, noWarranty: 0, model: r.componentModel };
          componentConsumption[sku].count++;
          if (v.includes('no warranty')) componentConsumption[sku].noWarranty++;
          else componentConsumption[sku].warranty++;
        }

        // Monthly
        const month = r.testDate ? r.testDate.substring(3, 10) : 'N/A';
        if (!monthlyConsumption[month]) monthlyConsumption[month] = {};
        for (const sku of skus) {
          if (!monthlyConsumption[month][sku]) monthlyConsumption[month][sku] = { total: 0, warranty: 0, noWarranty: 0 };
          monthlyConsumption[month][sku].total++;
          if (v.includes('no warranty')) monthlyConsumption[month][sku].noWarranty++;
          else monthlyConsumption[month][sku].warranty++;
        }
      }

      // Line lead time (using desk hoursInStatus if linked)
      if (r.fornecedor) {
        if (!slaByLine[r.fornecedor]) slaByLine[r.fornecedor] = { count: 0 };
        slaByLine[r.fornecedor].count++;
      }
    }

    // Sort products top 10
    const topProducts = Object.entries(productCount)
      .sort((a, b) => b[1].count - a[1].count)
      .slice(0, 10)
      .map(([model, d]) => ({ model, ...d }));

    const topFaults = Object.entries(faultCount)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 10)
      .map(([fault, count]) => ({ fault, count }));

    const topComponents = Object.entries(componentConsumption)
      .sort((a, b) => b[1].count - a[1].count)
      .slice(0, 20)
      .map(([sku, d]) => ({ sku, ...d }));

    res.json({
      updatedAt: new Date().toISOString(),
      period: { from: fromDate.toISOString(), to: toDate.toISOString() },
      desk: {
        total: deskTickets.length,
        slaByStatus,
        byTechnician: technicians,
        tickets: deskTickets.slice(0, 200)
      },
      rma: {
        total: rma.length,
        topProducts,
        topFaults,
        lineCount,
        serviceCount,
        warrantyCount,
        topComponents,
        monthlyConsumption
      },
      spareParts,
      sankhya: sankhya.slice(0, 500),
      dataStatus: {
        desk: deskTickets.length > 0,
        rma: (cache.rma.data || []).length > 0,
        sankhya: sankhya.length > 0,
        spareParts: spareParts.length > 0
      }
    });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

// Status check
app.get('/api/status', (req, res) => {
  res.json({
    desk: { loaded: !!cache.desk.data, count: cache.desk.data?.length || 0, ts: cache.desk.ts },
    rma: { loaded: !!cache.rma.data, count: cache.rma.data?.length || 0, ts: cache.rma.ts },
    sankhya: { loaded: !!cache.sankhya.data, count: cache.sankhya.data?.length || 0, ts: cache.sankhya.ts },
    sheets: { loaded: !!cache.sheets.data, count: cache.sheets.data?.length || 0, ts: cache.sheets.ts }
  });
});

app.use(express.static(path.join(__dirname, '../public')));

module.exports = app;
