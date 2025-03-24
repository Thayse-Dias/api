const express = require('express');
const axios = require('axios');
const app = express();
const port = 3000;

// Configurações do Dremio
const dremioHost = 'http://192.168.120.196:9047';
const dremioUser = 'bigdata';
const dremioPass = 'j[O0Y7eH3E5r';
let authToken = '';

// Middleware para parsear JSON
app.use(express.json());

// Função para autenticar no Dremio
async function authenticate() {
  try {
    const response = await axios.post(`${dremioHost}/api/v3/login`, {
      userName: dremioUser,
      password: dremioPass
    }, { timeout: 10000 });
    authToken = response.data.token;
    console.log('Autenticação bem-sucedida, token obtido.');
  } catch (error) {
    console.error('Erro ao autenticar no Dremio:', error.message);
    throw error;
  }
}

// Função para esperar o job completar
async function waitForJob(jobId) {
  let status = 'RUNNING';
  while (status === 'RUNNING') {
    const statusResponse = await axios.get(
      `${dremioHost}/api/v3/job/${jobId}`,
      { headers: { Authorization: `Bearer ${authToken}` } }
    );
    status = statusResponse.data.jobState;
    if (status === 'FAILED' || status === 'CANCELED') {
      throw new Error('Job no Dremio falhou ou foi cancelado');
    }
    await new Promise(resolve => setTimeout(resolve, 1000));
  }
}

// Função para executar uma query no Dremio
async function queryDremio(sql) {
  if (!authToken) await authenticate();

  try {
    const jobResponse = await axios.post(
      `${dremioHost}/api/v3/sql`,
      { sql },
      { headers: { Authorization: `Bearer ${authToken}` } }
    );
    const jobId = jobResponse.data.id;

    await waitForJob(jobId);

    const resultsResponse = await axios.get(
      `${dremioHost}/api/v3/job/${jobId}/results`,
      { headers: { Authorization: `Bearer ${authToken}` } }
    );
    return resultsResponse.data.rows;
  } catch (error) {
    console.error('Erro ao executar query no Dremio:', error.message);
    throw error;
  }
}

// Endpoint raiz (teste de conexão)
app.get('/', (req, res) => {
  res.send('API para Grafana funcionando');
});

// Endpoint de busca (lista de métricas/tabelas)
app.post('/search', (req, res) => {
  res.json(['user_status']); // Liste aqui as tabelas ou métricas disponíveis
});

// Endpoint de query (dados reais)
app.post('/query', async (req, res) => {
  try {
    const query = req.body.targets[0].target; // Pega a "tabela" ou query do Grafana
    const sql = `SELECT * FROM "${query}"`; // Ajuste conforme necessário
    const data = await queryDremio(sql);

    // Formato de tabela para o Simple JSON
    const response = [{
      columns: Object.keys(data[0]).map(key => ({ text: key })),
      rows: data.map(row => Object.values(row)),
      type: 'table'
    }];
    res.json(response);
  } catch (error) {
    console.error('Erro no endpoint /query:', error.message);
    res.status(500).json({ error: 'Erro ao consultar o Dremio' });
  }
});

// Inicia o servidor
app.listen(port, () => {
  console.log(`API rodando em http://localhost:${port}`);
});