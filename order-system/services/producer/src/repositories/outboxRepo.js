exports.insertOubox = async (client, { topic, key, payload }) => {
    const sql = `INSERT INTO outbox (aggregate_type, aggregate_id, topic, key, payload, created_at, published)
    VALUES ($1, $2, $3, $4, $5, now(), false)
    `

    await client.query(sql, ['order', key, topic, key, JSON.stringify(payload)]);
}