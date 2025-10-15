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

    console.log(`📩 Received event: ${eventId} for order ${orderId}`);

    const { rows } = await pool.query(
      "SELECT 1 FROM processed_events WHERE event_id = $1",
      [eventId]
    );
    if (rows.length > 0) {
      console.log(`⚠️ Duplicate event ${eventId}, skipping.`);
      return;
    }

    let attempts = 0;
    let success = false;

    while (attempts < 3 && !success) {
      const client = await pool.connect();
      try {
        await client.query("BEGIN"); //transaction start
        if (event.order_id === "O999") {
          throw new Error("Simulated DB failure for testing replay");
        }

        console.log(`🚀 Processing order ${orderId}, attempt ${attempts + 1}`);

        await client.query(
          `INSERT INTO orders (order_id, user_id, product_id, status, created_at)
           VALUES ($1, $2, $3, $4, now())
           ON CONFLICT (order_id) DO UPDATE
           SET status = EXCLUDED.status,
               user_id = EXCLUDED.user_id,
               product_id = EXCLUDED.product_id`,
          [
            event.order_id,
            event.user_id,
            event.product_id,
            event.status || "PENDING",
          ]
        );

        await client.query(
          `INSERT INTO processed_events (event_id, order_id, consumer)
           VALUES ($1, $2, $3)
           ON CONFLICT (event_id) DO NOTHING`,
          [eventId, orderId, "order-consumer"]
        );

        await client.query("COMMIT"); //transaction end
        success = true;
        console.log(`✅ Order ${orderId} processed successfully`);
      } catch (err) {
        await client.query("ROLLBACK"); //transaction rollback, if anything fails in above try block then rollback everythng.
        attempts++;

        if (
          err.message.includes("duplicate key value violates unique constraint")
        ) {
          console.log(`⚠️ Duplicate order ${orderId}, skipping reprocessing.`);
          success = true;
        } else {
          console.error(
            `❌ Error processing order ${orderId} (attempt ${attempts}): ${err.message}`
          );
        }

        if (!success && attempts >= 3) {
          if (event.replayed) {
            console.log(
              `Replay event ${eventId} failed again, NOT sending back to DLQ. Manual check required`
            );
          } else {
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
      } finally {
        client.release();
      }
    }
  },
});
