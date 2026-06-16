const axios = require('axios');
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

const STATUS_IGNORADOS = new Set([
  'Aguardando Chegada de Produto na Neosolar',
  'Descarte', 'Produto despachado AST', 'Produto despachado',
  'Aguardando Prazo / Autorização Descarte', 'Ag. Prazo / Autorização Descarte'
]);

// Técnicos AST — usados para identificar o responsável pelo ticket
const TECNICOS_AST = ['marcos miceli', 'nathan magri', 'wendel correa', 'marcos', 'nathan', 'wendel'];

async function getHistoryById(ticketId, token) {
  // Busca histórico completo com paginação
  let allEvents = [];
  let from = 0;
  while (true) {
    await new Promise(r => setTimeout(r, 100));
    const hist = await axios.get(
      `https://desk.zoho.com/api/v1/tickets/${ticketId}/History?limit=50&from=${from}`,
      { headers: { Authorization: `Zoho-oauthtoken ${token}` } }
    );
    const events = hist.data.data || [];
    allEvents = allEvents.concat(events);
    if (events.length < 50) break;
    from += 50;
  }

  const statusChanges = [];
  const ownerChanges  = [];
  let passouPorLaudo  = false;

  // allEvents vem do Zoho em ordem DECRESCENTE (mais recente primeiro)
  for (const e of allEvents) {
    if (!e.eventInfo) continue;
    for (const info of e.eventInfo) {
      // Mudanças de status
      if (info.propertyName === 'Status') {
        const val = info.propertyValue;
        const toStatus = val?.updatedValue || (typeof val === 'string' ? val : null);
        if (!toStatus) continue;
        if (toStatus === 'Aguardando laudo') passouPorLaudo = true;
        if (!STATUS_IGNORADOS.has(toStatus)) {
          statusChanges.push({ status: toStatus, from: val?.previousValue || null, time: e.eventTime });
        }
      }
      // Mudanças de proprietário/assignee
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

  // Técnico = identificado apenas se o ticket passou por "Aguardando laudo"
  let assigneeName = null;
  if (passouPorLaudo) {
    // Tentativa 1: proprietário no momento do laudo (campo Owner no histórico)
    const laudoEvent = statusChanges.find(s => s.status === 'Aguardando laudo');
    if (laudoEvent && ownerChanges.length > 0) {
      const laudoTime  = new Date(laudoEvent.time);
      const ownerNoLaudo = ownerChanges.filter(o => new Date(o.time) <= laudoTime).pop();
      if (ownerNoLaudo) {
        const ownerLower = ownerNoLaudo.owner.toLowerCase();
        if (TECNICOS_AST.some(t => ownerLower.includes(t))) {
          assigneeName = ownerNoLaudo.owner;
        }
      }
    }

    // Tentativa 2: último comentário feito por técnico AST
    // allEvents é decrescente — o primeiro CommentAdded de técnico encontrado é o mais recente
    if (!assigneeName) {
      for (const e of allEvents) {
        if (e.eventName !== 'CommentAdded') continue;
        const actorNome  = e.actor?.name || '';
        const actorLower = actorNome.toLowerCase();
        if (TECNICOS_AST.some(t => actorLower.includes(t))) {
          assigneeName = actorNome;
          break;
        }
      }
    }
  }

  return { statusTimes, statusChanges, passouPorLaudo, assigneeName, totalEvents: allEvents.length };
}

module.exports = async (req, res) => {
  const { ticketId, ticketNumber } = req.query;
  try {
    const token = await getDeskToken();

    // Modo 1: ticketId direto
    if (ticketId) {
      const result = await getHistoryById(ticketId, token);
      // createdTime/closedTime via ticket (paralelo)
      const ticketRes = await axios.get(
        `https://desk.zoho.com/api/v1/tickets/${ticketId}`,
        { headers: { Authorization: `Zoho-oauthtoken ${token}` } }
      ).catch(() => null);
      const createdTime = ticketRes?.data?.createdTime || null;
      const closedTime  = ticketRes?.data?.closedTime  || null;
      return res.json({ ticketId, createdTime, closedTime, ...result });
    }

    // Modo 2: busca por ticketNumber via Search API
    if (ticketNumber) {
      await new Promise(r => setTimeout(r, 100));
      const search = await axios.get(
        `https://desk.zoho.com/api/v1/tickets/search?ticketNumber=${ticketNumber}&departmentId=${process.env.ZOHO_DEPT_ID}`,
        { headers: { Authorization: `Zoho-oauthtoken ${token}` } }
      );
      const tickets = search.data.data || [];
      if (tickets.length === 0) {
        return res.json({ ticketNumber, found: false, statusTimes: {}, statusChanges: [], passouPorLaudo: false, assigneeName: null });
      }
      const id          = tickets[0].id;
      const createdTime = tickets[0].createdTime || null;
      const closedTime  = tickets[0].closedTime  || null;
      const result = await getHistoryById(id, token);
      return res.json({ ticketId: id, ticketNumber, createdTime, closedTime, ...result });
    }

    res.status(400).json({ error: 'ticketId or ticketNumber required' });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
};
