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

// Status ignorados no cálculo de lead time
const STATUS_IGNORADOS = new Set([
  'Aguardando Chegada de Produto na Neosolar',
  'Descarte',
  'Produto despachado AST',
  'Aguardando Prazo / Autorização Descarte',
  'Ag. Prazo / Autorização Descarte'
]);

module.exports = async (req, res) => {
  const { ticketId } = req.query;
  if (!ticketId) return res.status(400).json({ error: 'ticketId required' });

  try {
    const token = await getDeskToken();

    // Busca todos os eventos com paginação
    let allEvents = [];
    let from = 0;
    const limit = 50;
    while (true) {
      await new Promise(r => setTimeout(r, 100));
      const hist = await axios.get(
        `https://desk.zoho.com/api/v1/tickets/${ticketId}/History?limit=${limit}&from=${from}`,
        { headers: { Authorization: `Zoho-oauthtoken ${token}` } }
      );
      const events = hist.data.data || [];
      allEvents = allEvents.concat(events);
      if (events.length < limit) break;
      from += limit;
    }

    // Extrai mudanças de status (exclui status ignorados)
    let statusChanges = [];
    for (const e of allEvents) {
      if (!e.eventInfo) continue;
      for (const info of e.eventInfo) {
        if (info.propertyName === 'Status' && info.propertyValue) {
          const val = info.propertyValue;
          const toStatus = val.updatedValue || (typeof val === 'string' ? val : null);
          if (toStatus && !STATUS_IGNORADOS.has(toStatus)) {
            statusChanges.push({
              status: toStatus,
              from: val.previousValue || null,
              time: e.eventTime
            });
          }
        }
      }
    }

    statusChanges.sort((a, b) => new Date(a.time) - new Date(b.time));

    // Calcula tempo em cada status
    const statusTimes = {};
    for (let i = 0; i < statusChanges.length; i++) {
      const sName = statusChanges[i].status;
      const start = new Date(statusChanges[i].time);
      // Próxima mudança de status (pulando ignorados já foram filtrados)
      const end = i < statusChanges.length - 1
        ? new Date(statusChanges[i + 1].time)
        : new Date();
      const hours = (end - start) / 3600000;
      if (hours > 0 && hours < 8760) {
        statusTimes[sName] = (statusTimes[sName] || 0) + hours;
      }
    }

    res.json({ ticketId, statusTimes, statusChanges, totalEvents: allEvents.length });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
};
