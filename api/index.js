const express = require('express');
const axios = require('axios');
const cors = require('cors');
const app = express();
app.use(cors());
app.use(express.json());

const CLIENT_ID     = process.env.ZOHO_CLIENT_ID;
const CLIENT_SECRET = process.env.ZOHO_CLIENT_SECRET;
const REFRESH_TOKEN = process.env.ZOHO_REFRESH_TOKEN;
const DEPT_ID       = process.env.ZOHO_DEPT_ID;
const SUPA_URL      = process.env.SUPABASE_URL;
const SUPA_KEY      = process.env.SUPABASE_SERVICE_ROLE_KEY;

const productCache = {};
const sleep = ms => new Promise(r => setTimeout(r, ms));

async function getProductName(productId, token) {
  if (!productId) return null;
  if (productCache[productId]) return productCache[productId];
  try {
    await sleep(150);
    const r = await axios.get('https://desk.zoho.com/api/v1/products/' + productId,
      { headers: { Authorization: 'Zoho-oauthtoken ' + token } });
    const name = r.data.productName || r.data.name || null;
    productCache[productId] = name;
    return name;
  } catch (e) { return null; }
}

let accessToken = '';
let tokenExpiry = 0;

async function getToken() {
  if (Date.now() < tokenExpiry) return accessToken;
  const r = await axios.post(
    'https://accounts.zoho.com/oauth/v2/token' +
    '?refresh_token=' + REFRESH_TOKEN +
    '&client_id=' + CLIENT_ID +
    '&client_secret=' + CLIENT_SECRET +
    '&grant_type=refresh_token'
  );
  accessToken = r.data.access_token;
  tokenExpiry = Date.now() + (55 * 60 * 1000);
  return accessToken;
}

function normaliza(s) {
  return (s || '').toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '');
}

const STATUS_COM_HISTORICO = [
  'aguardando teste', 'em teste', 'em manutencao',
  'aguardando peca reposicao', 'em tratativa p/ devolucao cliente',
  'aguardando aprovacao de manutencao', 'aguardando manutencao',
  'aguardando laudo', 'em teste de estresse/ciclo', 'aguardando 1'
];

function precisaHistorico(status) {
  const n = normaliza(status);
  return STATUS_COM_HISTORICO.some(k => n.includes(k));
}

async function getStatusEntryTime(ticketId, currentStatus, token) {
  try {
    await sleep(150);
    const r = await axios.get(
      'https://desk.zoho.com/api/v1/tickets/' + ticketId + '/History',
      { headers: { Authorization: 'Zoho-oauthtoken ' + token } }
    );
    const events = r.data.data || [];
    const statusNorm = normaliza(currentStatus);
    let firstBlankStatusTime = null;
    for (const e of events) {
      if (!e.eventInfo) continue;
      for (const info of e.eventInfo) {
        if (info.propertyName !== 'Status') continue;
        const raw = info.propertyValue;
        if (raw && raw.updatedValue) {
          if (normaliza(raw.updatedValue) === statusNorm) return e.eventTime;
        } else if (typeof raw === 'string' && raw !== '') {
          if (normaliza(raw) === statusNorm) return e.eventTime;
        } else if (firstBlankStatusTime === null) {
          firstBlankStatusTime = e.eventTime;
        }
      }
    }
    return firstBlankStatusTime || null;
  } catch (e) { return null; }
}

let ticketsCache = null;
let ticketsCacheExpiry = 0;

app.get('/tickets', async (req, res) => {
  try {
    if (ticketsCache && Date.now() < ticketsCacheExpiry) {
      return res.json({ tickets: ticketsCache });
    }
    const token = await getToken();
    let all = [], start = 0;
    while (true) {
      const r = await axios.get(
        'https://desk.zoho.com/api/v1/tickets?departmentId=' + DEPT_ID +
        '&limit=50&from=' + start + '&include=assignee,contacts',
        { headers: { Authorization: 'Zoho-oauthtoken ' + token } }
      );
      const data = r.data.data || [];
      all = all.concat(data);
      if (data.length < 50) break;
      start += 50;
    }
    all = all.filter(t =>
      t.statusType !== 'Closed' &&
      normaliza(t.status) !== 'aguardando chegada de produto na neosolar'
    );
    const seen = new Map();
    all = all.filter(t => {
      if (seen.has(t.ticketNumber)) return false;
      seen.set(t.ticketNumber, true);
      return true;
    });
    for (const t of all.filter(t => precisaHistorico(t.status))) {
      t.statusEntryTime = await getStatusEntryTime(t.id, t.status, token) || t.modifiedTime || null;
    }
    for (const t of all) {
      t.productName = await getProductName(t.productId, token);
    }
    ticketsCache = all;
    ticketsCacheExpiry = Date.now() + (55 * 1000);
    res.json({ tickets: all });
  } catch (e) {
    res.status(500).json({ error: e.message, detail: e.response ? e.response.data : null });
  }
});

app.get('/closed-today', async (req, res) => {
  try {
    const token = await getToken();
    let start = 0, count = 0;
    const today = new Date(); today.setHours(0, 0, 0, 0);
    while (true) {
      const r = await axios.get(
        'https://desk.zoho.com/api/v1/tickets?departmentId=' + DEPT_ID +
        '&limit=50&from=' + start + '&sortBy=closedTime',
        { headers: { Authorization: 'Zoho-oauthtoken ' + token } }
      );
      const data = r.data.data || [];
      const fechadosHoje = data.filter(t => t.closedTime && new Date(t.closedTime) >= today);
      count += fechadosHoje.length;
      if (data.length < 50 || fechadosHoje.length === 0) break;
      start += 50;
    }
    res.json({ count });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── HELPERS SUPABASE ──
function supaHeaders() {
  return {
    apikey: SUPA_KEY,
    Authorization: 'Bearer ' + SUPA_KEY,
    'Content-Type': 'application/json',
    Prefer: 'resolution=merge-duplicates'
  };
}

function hojeDataBRT() {
  const brt = new Date(Date.now() - 3 * 60 * 60 * 1000);
  return brt.toISOString().slice(0, 10);
}

// Calcula minutos úteis entre dois timestamps (seg-sex, 09h-18h BRT = UTC-3)
// Abordagem simples: converte para BRT e opera em horário local BRT
function minutosUteisEntre(desde, ate) {
  if (!desde || !ate) return 0;
  const BRT_OFFSET = 3 * 60 * 60 * 1000; // UTC-3 em ms
  const inicio = new Date(new Date(desde).getTime() - BRT_OFFSET);
  const fim    = new Date(new Date(ate).getTime()   - BRT_OFFSET);
  if (fim <= inicio) return 0;

  let total = 0;
  let cursor = new Date(inicio);

  while (cursor < fim) {
    // getUTCDay() em BRT equivale ao dia local BRT
    const diaSemana = cursor.getUTCDay(); // 0=dom, 6=sab
    if (diaSemana !== 0 && diaSemana !== 6) {
      // 09h e 18h em BRT (representados como UTC após subtrair offset)
      const inicioUtil = new Date(cursor);
      inicioUtil.setUTCHours(9, 0, 0, 0);
      const fimUtil = new Date(cursor);
      fimUtil.setUTCHours(18, 0, 0, 0);

      const de   = cursor < inicioUtil ? inicioUtil : cursor;
      const ate2 = fim < fimUtil ? fim : fimUtil;
      if (ate2 > de) total += (ate2 - de) / 60000;
    }
    // Avança para próximo dia às 09h BRT
    cursor = new Date(cursor);
    cursor.setUTCDate(cursor.getUTCDate() + 1);
    cursor.setUTCHours(9, 0, 0, 0);
  }
  return Math.floor(total);
}

// ── BANCADA OCIOSIDADE ──

// Retorna o início do dia útil atual em BRT (09h00 BRT = 12h00 UTC)
function inicioDiaUtilBRT() {
  const brt = new Date(Date.now() - 3 * 60 * 60 * 1000);
  const dataHoje = brt.toISOString().slice(0, 10);
  // 09h BRT = 12h UTC
  return new Date(dataHoje + 'T12:00:00.000Z');
}

// GET: retorna livre_desde + total acumulado do dia (base + período atual)
// O período atual é limitado ao início do dia útil atual (09h BRT)
app.get('/bancada-ocioso', async (req, res) => {
  try {
    const hoje = hojeDataBRT();
    const [estadoRes, logRes] = await Promise.all([
      axios.get(SUPA_URL + '/rest/v1/bancada_ociosidade?select=tecnico,livre_desde',
        { headers: supaHeaders() }),
      axios.get(SUPA_URL + '/rest/v1/bancada_ociosidade_log?select=tecnico,minutos_base,minutos_ociosos&data=eq.' + hoje,
        { headers: supaHeaders() })
    ]);
    const estado = estadoRes.data || [];
    const log    = logRes.data   || [];
    const logMap = {};
    log.forEach(r => { logMap[r.tecnico] = r; });

    const agora = new Date().toISOString();
    const inicioDia = inicioDiaUtilBRT();

    const rows = estado.map(r => {
      const logRow = logMap[r.tecnico] || null;
      const base   = logRow ? (logRow.minutos_base || 0) : 0;

      let periodoAtual = 0;
      if (r.livre_desde) {
        // Limita o início do período ao começo do dia útil atual
        const livreDesde = new Date(r.livre_desde);
        const inicioEfetivo = livreDesde < inicioDia ? inicioDia : livreDesde;
        periodoAtual = minutosUteisEntre(inicioEfetivo.toISOString(), agora);
      }

      const minutos_hoje = base + periodoAtual;
      // Retorna livre_desde original para o frontend mostrar, mas o cálculo é correto
      return { tecnico: r.tecnico, livre_desde: r.livre_desde, minutos_hoje };
    });
    res.json({ ociosidade: rows });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// POST: técnico ficou disponível OU ficou ocupado
app.post('/bancada-ocioso', async (req, res) => {
  try {
    const { tecnico, livre_desde, ocupado } = req.body;
    if (!tecnico) return res.status(400).json({ error: 'tecnico required' });
    const agora = new Date().toISOString();
    const hoje  = hojeDataBRT();

    if (ocupado) {
      // Técnico ficou ocupado — consolida período no log e zera livre_desde
      // Limita o início ao começo do dia útil atual
      let minsInicio = livre_desde;
      if (livre_desde) {
        const ld = new Date(livre_desde);
        const inicioDia = inicioDiaUtilBRT();
        if (ld < inicioDia) minsInicio = inicioDia.toISOString();
      }
      const mins = minsInicio ? minutosUteisEntre(minsInicio, agora) : 0;

      // Zera livre_desde e registra evento
      await axios.post(SUPA_URL + '/rest/v1/bancada_ociosidade',
        { tecnico, livre_desde: null, updated_at: agora },
        { headers: supaHeaders() }
      );
      await axios.post(SUPA_URL + '/rest/v1/bancada_ociosidade_eventos',
        { tecnico, evento: 'ocupado', timestamp: agora },
        { headers: { ...supaHeaders(), Prefer: 'return=minimal' } }
      ).catch(e => console.error('evento ocupado error:', e.message));

      if (mins > 0) {
        // Busca base atual do dia
        const logRes = await axios.get(
          SUPA_URL + '/rest/v1/bancada_ociosidade_log?select=minutos_base&tecnico=eq.' + tecnico + '&data=eq.' + hoje,
          { headers: supaHeaders() }
        );
        const base = logRes.data && logRes.data[0] ? (logRes.data[0].minutos_base || 0) : 0;
        const novaBase = base + mins;
        await axios.post(SUPA_URL + '/rest/v1/bancada_ociosidade_log',
          { tecnico, data: hoje, minutos_base: novaBase, minutos_ociosos: novaBase, updated_at: agora },
          { headers: supaHeaders() }
        );
      }
    } else {
      // Técnico ficou disponível — salva livre_desde e registra evento
      const ts = livre_desde || agora;
      await axios.post(SUPA_URL + '/rest/v1/bancada_ociosidade',
        { tecnico, livre_desde: ts, updated_at: agora },
        { headers: supaHeaders() }
      );
      // Registra evento
      await axios.post(SUPA_URL + '/rest/v1/bancada_ociosidade_eventos',
        { tecnico, evento: 'disponivel', timestamp: ts },
        { headers: { ...supaHeaders(), Prefer: 'return=minimal' } }
      ).catch(e => console.error('evento disponivel error:', e.message));
    }
    res.json({ ok: true });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// ── BANCADA PRODUÇÃO ──

async function supaGetProducao() {
  try {
    const hoje = hojeDataBRT();
    const diaSemana = new Date().getDay();
    const diasDesdeSegunda = diaSemana === 0 ? 6 : diaSemana - 1;
    const inicioSemana = new Date(Date.now() - diasDesdeSegunda * 86400000);
    const inicioSemanaStr = new Date(inicioSemana.getTime() - 3*60*60*1000).toISOString().slice(0,10);
    const inicioMesStr = hoje.slice(0,7) + '-01';
    const r = await axios.get(
      SUPA_URL + '/rest/v1/bancada_producao_log?select=tecnico,data,produtos&data=gte.' + inicioMesStr,
      { headers: supaHeaders() }
    );
    const rows = r.data || [];
    const result = {};
    rows.forEach(row => {
      if (!result[row.tecnico]) result[row.tecnico] = { hoje: 0, semana: 0, mes: 0 };
      if (row.data === hoje)             result[row.tecnico].hoje   += row.produtos;
      if (row.data >= inicioSemanaStr)   result[row.tecnico].semana += row.produtos;
      result[row.tecnico].mes += row.produtos;
    });
    return result;
  } catch(e) { return {}; }
}

app.get('/bancada-producao', async (req, res) => {
  try {
    const producao = await supaGetProducao();
    res.json({ producao });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/bancada-producao', async (req, res) => {
  try {
    const { tecnico, quantidade } = req.body;
    if (!tecnico) return res.status(400).json({ error: 'tecnico required' });
    const hoje = hojeDataBRT();
    const r = await axios.get(
      SUPA_URL + '/rest/v1/bancada_producao_log?select=produtos&tecnico=eq.' + tecnico + '&data=eq.' + hoje,
      { headers: supaHeaders() }
    );
    const atual = r.data && r.data[0] ? r.data[0].produtos : 0;
    await axios.post(SUPA_URL + '/rest/v1/bancada_producao_log',
      { tecnico, data: hoje, produtos: atual + (quantidade || 1), updated_at: new Date().toISOString() },
      { headers: supaHeaders() }
    );
    res.json({ ok: true });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /bancada-eventos — retorna log de eventos dos últimos 7 dias
app.get('/bancada-eventos', async (req, res) => {
  try {
    const seteDiasAtras = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
    const r = await axios.get(
      SUPA_URL + '/rest/v1/bancada_ociosidade_eventos' +
      '?select=tecnico,evento,timestamp' +
      '&timestamp=gte.' + seteDiasAtras +
      '&order=timestamp.desc' +
      '&limit=200',
      { headers: supaHeaders() }
    );
    res.json({ eventos: r.data || [] });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

module.exports = app;
