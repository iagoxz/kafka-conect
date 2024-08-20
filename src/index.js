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
        const carro = JSON.parse(message.value);
        const query = `
            INSERT INTO carros (modelo, preco, foto) 
            VALUES ($1, $2, $3) RETURNING *`;
        const values = [carro.modelo, carro.preco, carro.foto];
        
        await pool.query(query, values);
        console.log('Carro inserido no banco de dados:', carro);
    } catch (error) {
        console.error('Erro ao processar mensagem do Kafka:', error);
    }
});

consumer.on('error', function (err) {
    console.error('Erro no Kafka Consumer:', err);
});

app.get('/nsei', async (req, res) => {
    try {
        const result = await pool.query('SELECT * FROM carros');
        res.json(result.rows);
    } catch (err) {
        console.error('Erro ao buscar carros:', err);
        res.status(500).json({ error: 'Erro ao buscar carros' });
    }
});


app.get('/nsei/buscar', async (req, res) => {
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

app.listen(port, () => {
    console.log(`API rodando na porta ${port}`);
});
