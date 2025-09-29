import pkg from 'pg';

const { Pool } = pkg;

const pool = new Pool({
  user: "myuser",
  host: "localhost",
  database: "ordersdb",
  password: "mypass",
  port: 5432,
});

export default pool;