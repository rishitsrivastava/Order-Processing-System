const pool = require('../db/pool');
const orderRepo = require('../repositories/orderRepo');
const outboxRepo = require('../repositories/outboxRepo');
const { v4: uuidv4 } = require('uuid');


exports.createOrder = async (req, res) => {
    const client = await pool.connect();
    try {
        await client.query('BEGIN');
        const orderId = orderData.orderId || `ordder-${Date.now()}`;
        await orderRepo.insertOrder(client, { ...orderData, orderId });
        const eventId = uuidv4();
        const event = {
            eventId,
            eventType: 'ORDER_BOOKED',
            orderId,
            payload: orderData,
            timestamp: new Date().toISOString()
        };
        await outboxRepo.insertOutbox(client, { topic: 'orders.main', key: orderId, payload: event });
        await client.query('COMMIT');
        return { status: 'accepted', eventId };
    } catch (err) {
        await client.query('ROLLBACK');
        throw err;
    } finally {
        client.release();
    }
}