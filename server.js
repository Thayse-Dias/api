const express = require('express');
const axios = require('axios');
require('dotenv').config(); // Carrega as variáveis de ambiente do arquivo .env
const app = express();
const port = 3000; // Porta fixa em 3000

// Configurações de data para filtros
const now = new Date();
const sevenDaysAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
const from = sevenDaysAgo.toISOString();
const to = now.toISOString();

// Configurações do Dremio
const dremioHost = 'http://192.168.120.196:9047';
const dremioUser = process.env.DREMIO_USER || 'bigdata'; // Usa .env ou valor padrão
const dremioPass = process.env.DREMIO_PASS || 'j[O0Y7eH3E5r'; // Usa .env ou valor padrão
let authToken = '';

// Middleware para parsear JSON
app.use(express.json());

/**
 * Função para autenticar no Dremio
 */
async function authenticate() {
  try {
    const response = await axios.post(
      `${dremioHost}/apiv2/login`,
      { userName: dremioUser, password: dremioPass },
      { timeout: 10000 }
    );
    authToken = response.data.token;
    console.log('Autenticação bem-sucedida, token obtido.');
  } catch (error) {
    console.error('Erro ao autenticar no Dremio:', error.message);
    throw error;
  }
}

/**
 * Função para esperar a conclusão de um job no Dremio
 * @param {string} jobId - ID do job a ser monitorado
 */
async function waitForJob(jobId) {
  let status = 'RUNNING';
  while (status === 'RUNNING') {
    const statusResponse = await axios.get(`${dremioHost}/api/v3/job/${jobId}`, {
      headers: { Authorization: `Bearer ${authToken}` },
      timeout: 10000,
    });
    status = statusResponse.data.jobState;
    if (status === 'FAILED' || status === 'CANCELED') {
      throw new Error(`Job no Dremio falhou ou foi cancelado: ${status}`);
    }
    console.log(`Aguardando job ${jobId}... Status: ${status}`);
    await new Promise((resolve) => setTimeout(resolve, 1000)); // Poll a cada 1 segundo
  }
  console.log(`Job ${jobId} completado com status: ${status}`);
}

/**
 * Função para executar uma query no Dremio
 * @param {string} sql - Query SQL a ser executada
 * @returns {Array} - Resultados da query
 */
async function queryDremio(sql) {
  if (!authToken) await authenticate();

  try {
    console.log('Executando query:', sql);
    console.log('Enviando requisição para Dremio com token:', authToken);
    const jobResponse = await axios.post(
      `${dremioHost}/api/v3/sql`,
      { sql },
      { headers: { Authorization: `Bearer ${authToken}` }, timeout: 10000 }
    );
    const jobId = jobResponse.data.id;
    console.log('Job criado com ID:', jobId);
    await waitForJob(jobId);
    const resultsResponse = await axios.get(
      `${dremioHost}/api/v3/job/${jobId}/results`,
      { headers: { Authorization: `Bearer ${authToken}` }, timeout: 10000 }
    );
    return resultsResponse.data.rows;
  } catch (error) {
    console.error('Erro ao executar query no Dremio:', error.message);
    if (error.response) {
      console.error('Detalhes do erro:', error.response.data);
    }
    throw error;
  }
}

// **Endpoint raiz (teste de conexão)**
app.get('/', (req, res) => {
  res.status(200).send('API para Grafana funcionando');
});

// **Endpoint de busca (lista de métricas/tabelas)**
app.post('/search', async (req, res) => {
  try {
    const tableName = 'user_status'; // Nome simplificado para o Grafana
    res.json([tableName]); // Retorna uma lista com o nome da tabela
  } catch (error) {
    console.error('Erro no endpoint /search:', error.message);
    res.status(500).json({ error: 'Erro ao listar tabelas' });
  }
});

// **Endpoint de query (dados reais)**
app.post('/query', async (req, res) => {
  try {
    const target = req.body.targets[0]?.target; // Pega o "target" enviado pelo Grafana
    if (!target) {
      return res.status(400).json({ error: 'Nenhum target especificado' });
    }

    // Gerar intervalo de datas (início e fim do dia atual)
    const now = new Date();
    const startOfDay = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 0, 0, 0);
    const endOfDay = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 23, 59, 59);
    const from = startOfDay.toISOString();
    const to = endOfDay.toISOString();

    // Mapeia o target para a query SQL
    let sql;
    if (target === 'user_status') {
      sql = `SELECT * FROM ConexaoOpa.suite."user_status" WHERE TO_DATE(startAt) BETWEEN '2025-03-24' AND '2025-03-25'`;
    } else {
      sql = `SELECT * FROM ConexaoOpa.suite."user_status" WHERE TO_DATE(startAt) BETWEEN '2025-03-24' AND '2025-03-25'`; // Fallback
    }

    const data = await queryDremio(sql);

    // Formato compatível com Simple JSON (tabela)
    const response = [
      {
        columns: Object.keys(data[0] || {}).map((key) => ({ text: key })),
        rows: data.map((row) => Object.values(row)),
        type: 'table',
      },
    ];
    res.json(response);
  } catch (error) {
    console.error('Erro no endpoint /query:', error.message);
    res.status(500).json({ error: 'Erro ao consultar o Dremio' });
  }
});

// **Endpoint /test-query (teste simples)**
app.get('/test-query', async (req, res) => {
  try {
    console.log('Iniciando consulta no /test-query...');
    const sql = `SELECT * FROM ConexaoOpa.suite."user_status" LIMIT 10`;
    console.log('SQL:', sql);
    const data = await queryDremio(sql);
    console.log('Dados recebidos:', data);
    res.json(data);
  } catch (error) {
    console.error('Erro no endpoint /test-query:', error.message);
    res.status(500).json({ error: 'Erro ao consultar o Dremio', details: error.message });
  }
});

// **Inicia o servidor**
app.listen(port, async () => {
  try {
    await authenticate(); // Autentica ao iniciar o servidor
    console.log(`API rodando em http://localhost:${port}`);
  } catch (error) {
    console.error('Falha ao iniciar o servidor devido à autenticação:', error.message);
  }
});