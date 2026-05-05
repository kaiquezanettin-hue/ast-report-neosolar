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
  'Descarte',
  'Produto despachado AST',
  'Produto despachado',
  'Aguardando Prazo / Autorização Descarte',
  'Ag. Prazo / Autorização Descarte'
]);

async function getHistoryById(ticketId, token) {
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

  let statusChanges = [];
  for (const e of allEvents) {
    if (!e.eventInfo) continue;
    for (const info of e.eventInfo) {
      if (info.propertyName === 'Status' && info.propertyValue) {
        const val = info.propertyValue;
        const toStatus = val.updatedValue || (typeof val === 'string' ? val : null);
        if (toStatus && !STATUS_IGNORADOS.has(toStatus)) {
          statusChanges.push({ status: toStatus, from: val.previousValue || null, time: e.eventTime });
        }
      }
    }
  }

  statusChanges.sort((a, b) => new Date(a.time) - new Date(b.time));

  const statusTimes = {};
  for (let i = 0; i < statusChanges.length; i++) {
    const sName = statusChanges[i].status;
    const start = new Date(statusChanges[i].time);
    const end = i < statusChanges.length - 1 ? new Date(statusChanges[i + 1].time) : new Date();
    const hours = (end - start) / 3600000;
    if (hours > 0 && hours < 8760) statusTimes[sName] = (statusTimes[sName] || 0) + hours;
  }

  return { statusTimes, statusChanges, totalEvents: allEvents.length };
}

module.exports = async (req, res) => {
  const { ticketId, ticketNumber } = req.query;

  try {
    const token = await getDeskToken();

    // Modo 1: ticketId direto
    if (ticketId) {
      const result = await getHistoryById(ticketId, token);
      return res.json({ ticketId, ...result });
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
        return res.json({ ticketNumber, found: false, statusTimes: {}, statusChanges: [] });
      }
      const id = tickets[0].id;
      const createdTime = tickets[0].createdTime;
      const closedTime = tickets[0].closedTime || null;
      const result = await getHistoryById(id, token);
      return res.json({ ticketId: id, ticketNumber, createdTime, closedTime, ...result });
    }

    res.status(400).json({ error: 'ticketId or ticketNumber required' });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
};
