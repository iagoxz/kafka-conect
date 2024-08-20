const {Pool} = require('pg');
require('dotenv').config({ path: '../.env' });

const pool = new Pool({
    user: process.env.PGUSER,
    host: process.env.PGHOST,
    database: process.env.PGDATABASE,
    password: process.env.PGPASSWORD,
    port: process.env.PGPORT,
});

const createTable = async () => {
    const query = `
    CREATE TABLE IF NOT EXISTS carros (
        id SERIAL PRIMARY KEY,
        modelo VARCHAR(50),
        preco DECIMAL(10, 2),
        foto TEXT
    );
`;

    try {
        await pool.query(query);
        console.log('Tabela carros criada com sucesso');
    } catch (err) {
        console.error('Erro ao criar a tabela carros', err);
    } finally {
        pool.end();
    }

};

createTable();