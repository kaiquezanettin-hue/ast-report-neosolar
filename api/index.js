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

// ── HELPERS ──
function supaHeaders() {
  return {
    apikey: SUPA_KEY,
    Authorization: 'Bearer ' + SUPA_KEY,
    'Content-Type': 'application/json',
    Prefer: 'resolution=merge-duplicates'
  };
}

// Data atual em BRT (YYYY-MM-DD)
function hojeDataBRT() {
  return new Date(Date.now() - 3*60*60*1000).toISOString().slice(0, 10);
}

// Início da semana atual (segunda) em BRT
function inicioSemanaDataBRT() {
  const agora = new Date(Date.now() - 3*60*60*1000);
  const diaSemana = agora.getDay(); // 0=dom
  const diasDesdeSegunda = diaSemana === 0 ? 6 : diaSemana - 1;
  const seg = new Date(agora);
  seg.setDate(agora.getDate() - diasDesdeSegunda);
  return seg.toISOString().slice(0, 10);
}

// Calcula minutos úteis entre dois timestamps em BRT (09h-18h, seg-sex)
// Recebe strings ISO UTC, converte para BRT internamente
function minutosUteisBRT(desde, ate) {
  if (!desde || !ate) return 0;

  // Converte para ms e aplica offset BRT (UTC-3)
  const msDesde = new Date(desde).getTime() - 3*60*60*1000;
  const msAte   = new Date(ate).getTime()   - 3*60*60*1000;
  if (msAte <= msDesde) return 0;

  let total = 0;
  // Cursor em ms BRT, começa no início do dia de 'desde'
  let cursorMs = msDesde;

  while (cursorMs < msAte) {
    const cursorDate = new Date(cursorMs);
    const diaSemana = cursorDate.getUTCDay(); // getUTCDay em BRT = dia correto

    if (diaSemana !== 0 && diaSemana !== 6) { // seg-sex
      // 09h e 18h em BRT = horas UTC do dia cursor
      const ano  = cursorDate.getUTCFullYear();
      const mes  = cursorDate.getUTCMonth();
      const dia  = cursorDate.getUTCDate();
      const ini9h  = Date.UTC(ano, mes, dia, 9,  0, 0); // 09h BRT em ms BRT
      const fim18h = Date.UTC(ano, mes, dia, 18, 0, 0); // 18h BRT em ms BRT

      const de  = Math.max(cursorMs, ini9h);
      const ate2 = Math.min(msAte, fim18h);
      if (ate2 > de) total += (ate2 - de) / 60000;
    }

    // Avança para próximo dia às 00h BRT
    const cursorDate2 = new Date(cursorMs);
    cursorDate2.setUTCDate(cursorDate2.getUTCDate() + 1);
    cursorDate2.setUTCHours(0, 0, 0, 0);
    cursorMs = cursorDate2.getTime();
  }

  return Math.floor(total);
}

// Início do dia atual às 09h BRT em ISO UTC
function inicio09hHojeBRT() {
  const hoje = hojeDataBRT(); // YYYY-MM-DD em BRT
  // 09h BRT = 12h UTC
  return hoje + 'T12:00:00.000Z';
}

// ── BANCADA OCIOSIDADE ──

// GET /bancada-ocioso
// Retorna para cada técnico:
//   livre_desde: timestamp de quando ficou disponível
//   mins_desde:  minutos úteis desde que ficou disponível (período atual)
//   mins_hoje:   minutos úteis disponível hoje (base gravada + período atual)
//   mins_semana: minutos úteis disponível na semana (base gravada + período atual)
app.get('/bancada-ocioso', async (req, res) => {
  try {
    const hoje        = hojeDataBRT();
    const inicioSemana = inicioSemanaDataBRT();
    const agora       = new Date().toISOString();
    const ini09h      = inicio09hHojeBRT();

    const [estadoRes, logRes] = await Promise.all([
      axios.get(SUPA_URL + '/rest/v1/bancada_ociosidade?select=tecnico,livre_desde',
        { headers: supaHeaders() }),
      axios.get(SUPA_URL + '/rest/v1/bancada_ociosidade_log?select=tecnico,data,minutos_base,minutos_base_semana&data=gte.' + inicioSemana,
        { headers: supaHeaders() })
    ]);

    const estado = estadoRes.data || [];
    const logs   = logRes.data   || [];

    // Monta mapas: base_hoje e base_semana por técnico
    const baseHoje   = {};
    const baseSemana = {};
    logs.forEach(r => {
      if (!baseSemana[r.tecnico]) baseSemana[r.tecnico] = 0;
      baseSemana[r.tecnico] += (r.minutos_base_semana || r.minutos_base || 0);
      if (r.data === hoje) {
        baseHoje[r.tecnico] = r.minutos_base || 0;
      }
    });

    const rows = estado.map(r => {
      const ld = r.livre_desde;
      let mins_desde = 0, mins_hoje_periodo = 0, mins_semana_periodo = 0;

      if (ld) {
        // Período atual desde que ficou disponível
        mins_desde = minutosUteisBRT(ld, agora);
        // Período atual limitado a hoje (09h até agora)
        const inicioEfetivoHoje = ld > ini09h ? ld : ini09h;
        mins_hoje_periodo   = minutosUteisBRT(inicioEfetivoHoje, agora);
        // Período atual para a semana (desde segunda ou desde que ficou disponível)
        const inicioEfetivoSemana = ld > (inicioSemana + 'T12:00:00.000Z') ? ld : (inicioSemana + 'T12:00:00.000Z');
        mins_semana_periodo = minutosUteisBRT(inicioEfetivoSemana, agora);
      }

      return {
        tecnico:     r.tecnico,
        livre_desde: ld,
        mins_desde,
        mins_hoje:   (baseHoje[r.tecnico]   || 0) + mins_hoje_periodo,
        mins_semana: (baseSemana[r.tecnico]  || 0) + mins_semana_periodo
      };
    });

    res.json({ ociosidade: rows });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /bancada-ocioso
app.post('/bancada-ocioso', async (req, res) => {
  try {
    const { tecnico, livre_desde, ocupado } = req.body;
    if (!tecnico) return res.status(400).json({ error: 'tecnico required' });
    const agora = new Date().toISOString();
    const hoje  = hojeDataBRT();
    const inicioSemana = inicioSemanaDataBRT();
    const ini09h = inicio09hHojeBRT();

    if (ocupado && livre_desde) {
      // Consolida período encerrado no log
      const inicioEfetivoHoje   = livre_desde > ini09h ? livre_desde : ini09h;
      const inicioEfetivoSemana = livre_desde > (inicioSemana + 'T12:00:00.000Z') ? livre_desde : (inicioSemana + 'T12:00:00.000Z');
      const minsHoje   = minutosUteisBRT(inicioEfetivoHoje, agora);
      const minsSemana = minutosUteisBRT(inicioEfetivoSemana, agora);

      // Zera livre_desde
      await axios.post(SUPA_URL + '/rest/v1/bancada_ociosidade',
        { tecnico, livre_desde: null, updated_at: agora },
        { headers: supaHeaders() }
      );

      // Registra evento
      await axios.post(SUPA_URL + '/rest/v1/bancada_ociosidade_eventos',
        { tecnico, evento: 'ocupado', timestamp: agora },
        { headers: { ...supaHeaders(), Prefer: 'return=minimal' } }
      ).catch(() => {});

      if (minsHoje > 0 || minsSemana > 0) {
        const logRes = await axios.get(
          SUPA_URL + '/rest/v1/bancada_ociosidade_log?select=minutos_base,minutos_base_semana&tecnico=eq.' + tecnico + '&data=eq.' + hoje,
          { headers: supaHeaders() }
        );
        const row = logRes.data && logRes.data[0] ? logRes.data[0] : null;
        const baseHoje   = row ? (row.minutos_base         || 0) : 0;
        const baseSemana = row ? (row.minutos_base_semana   || 0) : 0;
        await axios.post(SUPA_URL + '/rest/v1/bancada_ociosidade_log',
          { tecnico, data: hoje, minutos_base: baseHoje + minsHoje, minutos_base_semana: baseSemana + minsSemana, updated_at: agora },
          { headers: supaHeaders() }
        );
      }
    } else {
      // Técnico ficou disponível
      const ts = livre_desde || agora;
      await axios.post(SUPA_URL + '/rest/v1/bancada_ociosidade',
        { tecnico, livre_desde: ts, updated_at: agora },
        { headers: supaHeaders() }
      );
      await axios.post(SUPA_URL + '/rest/v1/bancada_ociosidade_eventos',
        { tecnico, evento: 'disponivel', timestamp: ts },
        { headers: { ...supaHeaders(), Prefer: 'return=minimal' } }
      ).catch(() => {});
    }
    res.json({ ok: true });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// ── BANCADA PRODUÇÃO ──
app.get('/bancada-producao', async (req, res) => {
  try {
    const hoje = hojeDataBRT();
    const inicioSemana = inicioSemanaDataBRT();
    const inicioMes = hoje.slice(0,7) + '-01';
    const r = await axios.get(
      SUPA_URL + '/rest/v1/bancada_producao_log?select=tecnico,data,produtos&data=gte.' + inicioMes,
      { headers: supaHeaders() }
    );
    const rows = r.data || [];
    const result = {};
    rows.forEach(row => {
      if (!result[row.tecnico]) result[row.tecnico] = { hoje: 0, semana: 0, mes: 0 };
      if (row.data === hoje)             result[row.tecnico].hoje   += row.produtos;
      if (row.data >= inicioSemana)      result[row.tecnico].semana += row.produtos;
      result[row.tecnico].mes += row.produtos;
    });
    res.json({ producao: result });
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

// ── EVENTOS ──
app.get('/bancada-eventos', async (req, res) => {
  try {
    const seteDiasAtras = new Date(Date.now() - 7*24*60*60*1000).toISOString();
    const r = await axios.get(
      SUPA_URL + '/rest/v1/bancada_ociosidade_eventos?select=tecnico,evento,timestamp&timestamp=gte.' + seteDiasAtras + '&order=timestamp.desc&limit=200',
      { headers: supaHeaders() }
    );
    res.json({ eventos: r.data || [] });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

module.exports = app;
