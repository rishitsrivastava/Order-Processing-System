import { Kafka } from "kafkajs";
import pool from "./db.js";

const kafka = new Kafka({
  clientId: "order-audit-service",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({
  groupId: "order-audit-group",
  autoCommit: false,
});

async function startAuditService() {
  await consumer.connect();
  await consumer.subscribe({ topic: "orders.main", fromBeginning: false });

  console.log("🧾 Audit service running... Listening to 'orders.main'");

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message, pause, heartbeat }) => {
      try {
        const event = JSON.parse(message.value.toString());
        const eventId = event.event_id;
        const orderId = event.order_id;
        const eventType = event.event || "UNKNOWN";
        const status = event.status || "N/A";

        console.log(
          `📝 Auditing event ${eventId} [${eventType}] for order ${orderId}`
        );

        await pool.query(
          `INSERT INTO order_events_audit (event_id, order_id, event_type, status, payload)
           VALUES ($1, $2, $3, $4, $5)
           ON CONFLICT DO NOTHING`,
          [eventId, orderId, eventType, status, event]
        );

        await consumer.commitOffsets([
          {
            topic,
            partition,
            offset: (Number(message.offset) + 1).toString(),
          },
        ]);
      } catch (err) {
        console.error("❌ Audit service error:", err.message);
        pause();
        setTimeout(
          () => consumer.resume([{ topic, partitions: [partition] }]),
          5000
        );
      }
      await heartbeat();
    },
  });
}

startAuditService().catch((err) => {
  console.error("Audit service failed:", err);
});
