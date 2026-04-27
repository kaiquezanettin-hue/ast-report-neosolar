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
  const { ticketNumber } = req.query;
  if (!ticketNumber) return res.status(400).json({ error: 'ticketNumber required' });

  try {
    const token = await getDeskToken();
    await new Promise(r => setTimeout(r, 100));

    const response = await axios.get(
      `https://desk.zoho.com/api/v1/tickets?ticketNumber=${ticketNumber}&departmentId=${process.env.ZOHO_DEPT_ID}`,
      { headers: { Authorization: `Zoho-oauthtoken ${token}` } }
    );

    const tickets = response.data.data || [];
    if (tickets.length === 0) {
      return res.json({ found: false, ticketNumber });
    }

    const t = tickets[0];
    res.json({
      found: true,
      ticketNumber: t.ticketNumber,
      ticketId: t.id,
      status: t.status,
      closedTime: t.closedTime || null,
      createdTime: t.createdTime
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
};
