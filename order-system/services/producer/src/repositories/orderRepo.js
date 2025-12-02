exports.insertOrder = async (client, order) => {
  const sql = `INSERT INTO orders (order_id, customer_id, payload, status, created_at) 
  VALUES ($1, $2, $3, $4, now())`;
  await client.query(sql, [
    order.orderId,
    order.customerId || null,
    JSON.stringify(order),
    order.status || "BOOKED",
  ]);
};
