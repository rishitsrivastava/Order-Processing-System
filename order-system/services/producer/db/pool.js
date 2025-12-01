const { Pool } = require('pg');

const pool = new Pool({
    host: process.env.PG_HOST || 'localhost',
    user: process.env.PG_user || 'postgres',
    password: process.env.PG_PASS || 'postgres',
    database: process.env.PG_DB || 'ordersdb',
    max: Number(process.env.PG_MAX_CONN || 20)
})

module.exports = pool