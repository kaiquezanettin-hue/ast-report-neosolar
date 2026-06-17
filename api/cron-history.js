const axios = require('axios');

// ─── Supabase ────────────────────────────────────────────────────────
const SUPA_URL = process.env.SUPABASE_URL;
const SUPA_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

async function dbGet(key) {
  try {
    const res = await axios.get(
      `${SUPA_URL}/rest/v1/ast_storage?key=eq.${encodeURIComponent(key)}&select=value,updated_at`,
      { headers: { apikey: SUPA_KEY, Authorization: `Bearer ${SUPA_KEY}` } }
    );
    if (res.data && res.data.length > 0) return JSON.parse(res.data[0].value);
    return null;
  } catch { return null; }
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

const delay = ms => new Promise(r => setTimeout(r, ms));

const STATUS_IGNORADOS = new Set([
  'Aguardando Chegada de Produto na Neosolar',
  'Descarte', 'Produto despachado AST', 'Produto despachado',
  'Aguardando Prazo / Autorização Descarte', 'Ag. Prazo / Autorização Descarte'
]);

const TECNICOS_AST = ['marcos miceli', 'nathan magri', 'wendel correa', 'marcos', 'nathan', 'wendel'];

// ─── Busca histórico completo de um ticket ───────────────────────────
async function getTicketHistory(ticketId, token) {
  let allEvents = [];
  let from = 0;
  while (true) {
    await delay(120);
    try {
      const res = await axios.get(
        `https://desk.zoho.com/api/v1/tickets/${ticketId}/History?limit=50&from=${from}`,
        { headers: { Authorization: `Zoho-oauthtoken ${token}` } }
      );
      const batch = res.data.data || [];
      allEvents = allEvents.concat(batch);
      if (batch.length < 50) break;
      from += 50;
    } catch { break; }
  }

  const statusChanges = [];
  const ownerChanges  = [];
  let passouPorLaudo  = false;

  for (const e of allEvents) {
    if (!e.eventInfo) continue;
    for (const info of e.eventInfo) {
      if (info.propertyName === 'Status') {
        const val = info.propertyValue;
        const toStatus = val?.updatedValue || (typeof val === 'string' ? val : null);
        if (!toStatus) continue;
        if (toStatus === 'Aguardando laudo') passouPorLaudo = true;
        if (!STATUS_IGNORADOS.has(toStatus)) {
          statusChanges.push({ status: toStatus, from: val?.previousValue || null, time: e.eventTime });
        }
      }
      if (['Owner','Assignee','ownerId','assigneeId'].includes(info.propertyName)) {
        const val = info.propertyValue;
        const newOwner = val?.updatedValue || val?.name || (typeof val === 'string' ? val : null);
        if (newOwner) ownerChanges.push({ owner: newOwner, time: e.eventTime });
      }
    }
  }

  statusChanges.sort((a, b) => new Date(a.time) - new Date(b.time));
  ownerChanges.sort((a, b) => new Date(a.time) - new Date(b.time));

  const statusTimes = {};
  for (let i = 0; i < statusChanges.length; i++) {
    const sName = statusChanges[i].status;
    const start = new Date(statusChanges[i].time);
    const end   = i < statusChanges.length - 1 ? new Date(statusChanges[i + 1].time) : new Date();
    const hours = (end - start) / 3600000;
    if (hours > 0 && hours < 8760) statusTimes[sName] = (statusTimes[sName] || 0) + hours;
  }

  // Técnico: proprietário no momento do laudo ou último comentário de técnico AST
  let assigneeName = null;
  if (passouPorLaudo) {
    const laudoEvent = statusChanges.find(s => s.status === 'Aguardando laudo');
    if (laudoEvent && ownerChanges.length > 0) {
      const laudoTime    = new Date(laudoEvent.time);
      const ownerNoLaudo = ownerChanges.filter(o => new Date(o.time) <= laudoTime).pop();
      if (ownerNoLaudo) {
        const ownerLower = ownerNoLaudo.owner.toLowerCase();
        if (TECNICOS_AST.some(t => ownerLower.includes(t))) assigneeName = ownerNoLaudo.owner;
      }
    }
    if (!assigneeName) {
      for (const e of allEvents) {
        if (e.eventName !== 'CommentAdded') continue;
        const actorNome  = e.actor?.name || '';
        const actorLower = actorNome.toLowerCase();
        if (TECNICOS_AST.some(t => actorLower.includes(t))) { assigneeName = actorNome; break; }
      }
    }
  }

  return { statusTimes, statusChanges, passouPorLaudo, assigneeName, totalEvents: allEvents.length };
}

// ─── Endpoint principal do cron ──────────────────────────────────────
module.exports = async (req, res) => {
  // Segurança: só aceita chamadas do próprio Vercel Cron ou com token
  const authHeader = req.headers.authorization || '';
  const cronSecret = process.env.CRON_SECRET || '';
  if (cronSecret && authHeader !== `Bearer ${cronSecret}`) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const startTime = Date.now();
  console.log(`[cron-history] Iniciando às ${new Date().toISOString()}`);

  try {
    const token = await getDeskToken();

    // 1. Carrega histórico existente do Supabase
    const existing = (await dbGet('desk_history_cache')) || {};
    console.log(`[cron-history] Histórico existente: ${Object.keys(existing).length} tickets`);

    // 2. Busca todos os tickets do Desk
    let allTickets = [];
    let from = 0;
    while (true) {
      await delay(200);
      const res2 = await axios.get(
        `https://desk.zoho.com/api/v1/tickets?departmentId=${process.env.ZOHO_DEPT_ID}&limit=50&from=${from}&include=assignee&sortBy=-createdTime`,
        { headers: { Authorization: `Zoho-oauthtoken ${token}` } }
      );
      const batch = res2.data.data || [];
      allTickets = allTickets.concat(batch);
      if (batch.length < 50) break;
      from += 50;
    }
    console.log(`[cron-history] Total tickets Desk: ${allTickets.length}`);

    // 3. Busca histórico de cada ticket — só os que mudaram desde o último cache
    let processed = 0, updated = 0, errors = 0;
    const newCache = { ...existing };

    // Processa tickets em ordem do mais recente para o mais antigo
    // Para tickets já no cache, só re-processa se foram modificados recentemente
    const ontem = new Date(); ontem.setDate(ontem.getDate() - 1);

    for (const t of allTickets) {
      const id = t.id;
      const modifiedTime = new Date(t.modifiedTime || t.createdTime);
      const jaTemCache   = !!existing[id];
      const foiModificado = modifiedTime >= ontem;

      // Pula se já tem cache E não foi modificado nas últimas 24h E não está aberto
      if (jaTemCache && !foiModificado && t.statusType === 'Closed') {
        processed++;
        continue;
      }

      try {
        const hist = await getTicketHistory(id, token);
        if (hist.statusTimes && Object.keys(hist.statusTimes).length > 0) {
          newCache[id] = {
            ...hist,
            ticketNumber: t.ticketNumber,
            createdTime:  t.createdTime,
            closedTime:   t.closedTime  || null,
            modifiedTime: t.modifiedTime || null,
            assigneeName: hist.assigneeName || t.assignee?.name || null
          };
          updated++;
        }
      } catch (e) {
        console.error(`[cron-history] Erro ticket ${t.ticketNumber}:`, e.message);
        errors++;
      }

      processed++;
      if (processed % 20 === 0) {
        console.log(`[cron-history] Progresso: ${processed}/${allTickets.length} | Atualizados: ${updated}`);
        // Salva checkpoint a cada 20 tickets para não perder progresso
        await dbSet('desk_history_cache', newCache);
      }

      // Timeout de segurança: Vercel tem limite de 60s nas funções serverless
      // Salva e encerra se estiver próximo do limite
      if (Date.now() - startTime > 50000) {
        console.log('[cron-history] Timeout preventivo — salvando progresso');
        await dbSet('desk_history_cache', newCache);
        await dbSet('desk_history_meta', {
          lastRun: new Date().toISOString(),
          totalTickets: allTickets.length,
          processed, updated, errors,
          status: 'partial'
        });
        return res.json({ ok: true, status: 'partial', processed, updated, errors, totalTickets: allTickets.length });
      }
    }

    // 4. Salva cache final no Supabase
    await dbSet('desk_history_cache', newCache);
    await dbSet('desk_history_meta', {
      lastRun: new Date().toISOString(),
      totalTickets: allTickets.length,
      processed, updated, errors,
      cacheSize: Object.keys(newCache).length,
      status: 'complete'
    });

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`[cron-history] Concluído em ${elapsed}s | Processados: ${processed} | Atualizados: ${updated} | Erros: ${errors}`);

    res.json({ ok: true, status: 'complete', processed, updated, errors, totalTickets: allTickets.length, elapsed: `${elapsed}s` });

  } catch (e) {
    console.error('[cron-history] Erro fatal:', e.message);
    res.status(500).json({ error: e.message });
  }
};
