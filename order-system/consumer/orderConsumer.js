import { Kafka } from "kafkajs";
import pool from "../db.js";

const kafka = new Kafka({
  clientId: "order-consumer",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({ groupId: "order-group" });
const producer = kafka.producer();

await consumer.connect();
await producer.connect();

await consumer.subscribe({ topic: "orders.main", fromBeginning: true });

console.log("✅ Order consumer is running and subscribed to 'orders.main'...");

consumer.run({
  eachMessage: async ({ topic, partition, message }) => {
    const event = JSON.parse(message.value.toString());
    const eventId = event.event_id;
    const orderId = event.order_id;

    try {
      const { rows } = await pool.query(
        "SELECT 1 FROM processed_events WHERE event_id = $1",
        [eventId]
      );
      if (rows.length > 0) {
        console.log(
          `⚠️ Duplicate event detected: ${eventId}, skipping processing.`
        );
        return;
      }
      let attempts = 0;
      let success = false;
      while (attempts < 3 && !success) {
        try {
          console.log(
            `🚀 Processing order ${orderId}, attempt ${attempts + 1}`
          );
          await pool.query(
            "INSERT INTO orders (order_id, status, created_at) VALUES ($1, $2, now())",
            [orderId, event.status || "PENDING"]
          );
          success = true;
          await pool.query(
            "INSERT INTO processed_events (event_id, order_id, consumer) VALUES ($1, $2, $3)",
            [eventId, orderId, "order-consumer"]
          );

          console.log(`✅ Order ${orderId} processed successfully`);
        } catch (err) {
          attempts++;
          console.error(
            `❌ Error processing order ${orderId} (attempt ${attempts}): ${err.message}`
          );

          if (attempts >= 3) {
            console.log(
              `🚨 Moving event ${eventId} to DLQ after 3 failed attempts...`
            );
            await producer.send({
              topic: "orders.DLQ",
              messages: [
                {
                  key: eventId,
                  value: JSON.stringify({
                    eventId,
                    orderId,
                    error: err.message,
                    payload: event,
                    timestamp: new Date().toISOString(),
                  }),
                },
              ],
            });
          }
        }
      }
    } catch (err) {
      console.log(
        `⚠️ Unexpected failure handling event ${eventId}: ${err.message}`
      );
    }
  },
})