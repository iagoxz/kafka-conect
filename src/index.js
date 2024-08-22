const express = require('express');
const kafka = require('kafka-node');
const { Pool } = require('pg');
require('dotenv').config({ path: '../.env' });


const app = express();
const port = process.env.PORT || 3000;

const kafkaClientOptions = {
    kafkaHost: process.env.KAFKA_BROKER
};



const client = new kafka.KafkaClient(kafkaClientOptions);
const consumer = new kafka.Consumer(
    client,
    [{ topic: process.env.KAFKA_TOPIC, partition: 0 }],
    { autoCommit: true }
);


const pool = new Pool();

consumer.on('message', async function (message) {
    try {
        console.log('Mensagem recebida do Kafka:', message.value);

        const carro = JSON.parse(message.value);
        console.log('Dados do carro:', carro);

        const modeloCompleto = carro.Modelo || carro.modelo;
        const precoOriginal = carro['Preço'] || carro.preco;
        const foto = carro.Foto || carro.foto;

        const modelo = modeloCompleto.split(' ').slice(1).join(' ');

        const preco = parseFloat(precoOriginal.replace('R$', '').replace('.', '').replace(',', '.'));

        if (!modelo || isNaN(preco) || !foto) {
            console.error('Dados incompletos ou inválidos:', carro);
            return;  // Evita inserir dados incompletos ou inválidos
        }

        const query = `
            INSERT INTO carros (modelo, preco, foto) 
            VALUES ($1, $2, $3) RETURNING *`;
        const values = [modelo, preco, foto];

        const result = await pool.query(query, values);
        console.log('Carro inserido no banco de dados:', result.rows[0]);
    } catch (error) {
        console.error('Erro ao processar mensagem do Kafka:', error);
    }
});

consumer.on('error', function (err) {
    console.error('Erro no Kafka Consumer:', err);
});

app.get('/carros', async (req, res) => {
    try {
        const result = await pool.query('SELECT * FROM carros');
        res.json(result.rows);
    } catch (err) {
        console.error('Erro ao buscar carros:', err);
        res.status(500).json({ error: 'Erro ao buscar carros' });
    }
});


app.get('/carros/buscar', async (req, res) => {
    const { modelo } = req.query;

    if (!modelo) {
        return res.status(400).json({ error: 'O parâmetro "modelo" é obrigatório.' });
    }

    let query = 'SELECT * FROM carros WHERE LOWER(modelo) LIKE $1';
    const values = [`%${modelo.toLowerCase()}%`];

    try {
        const result = await pool.query(query, values);
        res.json(result.rows);
    } catch (err) {
        console.error('Erro ao pesquisar carros:', err);
        res.status(500).json({ error: 'Erro ao pesquisar carros' });
    }
});

app.get('/carros/preco/maior', async (req, res) => {
    const { valor } = req.query;

    if (!valor || isNaN(valor)) {
        return res.status(400).json({ error: 'O parâmetro "valor" é obrigatório e deve ser um número.' });
    }

    try {
        const result = await pool.query('SELECT * FROM carros WHERE preco > $1', [parseFloat(valor)]);
        res.json(result.rows);
    } catch (err) {
        console.error('Erro ao buscar carros com preço maior:', err);
        res.status(500).json({ error: 'Erro ao buscar carros com preço maior' });
    }
});

app.get('/carros/preco/menor-igual', async (req, res) => {
    const { valor } = req.query;

    if (!valor || isNaN(valor)) {
        return res.status(400).json({ error: 'O parâmetro "valor" é obrigatório e deve ser um número.' });
    }

    try {
        const result = await pool.query('SELECT * FROM carros WHERE preco <= $1', [parseFloat(valor)]);
        res.json(result.rows);
    } catch (err) {
        console.error('Erro ao buscar carros com preço menor ou igual:', err);
        res.status(500).json({ error: 'Erro ao buscar carros com preço menor ou igual' });
    }
});

app.listen(port, () => {
    console.log(`API rodando a porta ${port}`);
});
