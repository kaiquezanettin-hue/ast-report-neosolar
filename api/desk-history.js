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

module.exports = async (req, res) => {
  const { ticketId } = req.query;
  if (!ticketId) return res.status(400).json({ error: 'ticketId required' });

  try {
    const token = await getDeskToken();
    await new Promise(r => setTimeout(r, 150));

    const hist = await axios.get(
      `https://desk.zoho.com/api/v1/tickets/${ticketId}/History`,
      { headers: { Authorization: `Zoho-oauthtoken ${token}` } }
    );

    const events = hist.data.data || [];

    // Extrai mudanças de status — tenta todos os formatos conhecidos do Zoho
    let statusChanges = [];
    for (const e of events) {
      // Formato novo: fieldName/from/to
      if (e.fieldName === 'Status' && e.to) {
        statusChanges.push({ status: e.to, time: e.modifiedTime });
        continue;
      }
      // Formato antigo: eventInfo array
      if (e.eventInfo && Array.isArray(e.eventInfo)) {
        for (const info of e.eventInfo) {
          if (info.propertyName !== 'Status') continue;
          const val = info.propertyValue;
          const toStatus = val?.updatedValue || (typeof val === 'string' ? val : null);
          if (toStatus) statusChanges.push({ status: toStatus, time: e.eventTime });
        }
      }
    }

    statusChanges.sort((a, b) => new Date(a.time) - new Date(b.time));

    // Calcula tempo em cada status
    const statusTimes = {};
    for (let i = 0; i < statusChanges.length; i++) {
      const sName = statusChanges[i].status;
      const start = new Date(statusChanges[i].time);
      const end = i < statusChanges.length - 1
        ? new Date(statusChanges[i + 1].time)
        : new Date();
      const hours = (end - start) / 3600000;
      if (hours > 0 && hours < 8760) {
        statusTimes[sName] = (statusTimes[sName] || 0) + hours;
      }
    }

    // Sempre retorna debug completo
    return res.json({
      ticketId,
      statusTimes,
      statusChanges,
      debug: {
        eventsCount: events.length,
        events: events.slice(0, 3)
      }
    });

    res.json({ ticketId, statusTimes, statusChanges });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
};
