import { Kafka } from "kafkajs";
import pool from "../db.js";

const kafka = new Kafka({
  clientId: "order-consumer",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({ groupId: "order-group" });
const producer = kafka.producer(); 

const runOrderConsumer = async () => {
  await consumer.connect();
  await producer.connect();
  await consumer.subscribe({ topic: "orders", fromBeginning: true });

  console.log("🧾 OrderConsumer running...");

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const order = JSON.parse(message.value.toString());
        const { event, order_id, user_id, product_id } = order;

        console.log("📨 Received event: ", event, order_id);

        switch (event) {
          case "ORDER_BOOKED":
            await pool.query(
              `INSERT INTO orders (order_id, user_id, product_id, status, created_at)
               VALUES ($1, $2, $3, $4, NOW())
               ON CONFLICT (order_id) DO UPDATE SET status = EXCLUDED.status`,
              [order_id, user_id, product_id, "BOOKED"]
            );
            console.log(`✅ Order ${order_id} booked.`);

            await producer.send({
              topic: "orders",
              messages: [
                {
                  value: JSON.stringify({
                    event: "ORDER_SHIPPED",
                    order_id,
                    user_id,
                    product_id,
                    status: "SHIPPED",
                  }),
                },
              ],
            });
            console.log(`Emitted ORDER_SHIPPED for ${order_id}`);
            break;

          case "ORDER_CANCELLED":
            await pool.query(
              "UPDATE orders SET status = $1 WHERE order_id = $2",
              ["CANCELLED", order_id]
            );
            console.log(`Order ${order_id} cancelled`);
            break;

          default:
            // Do nothing for other events
            break;
        }
      } catch (err) {
        console.error("Error processing message:", err);
      }
    },
  });
};

if (process.argv[1].includes("orderConsumer.js")) {
  runOrderConsumer().catch(console.error);
}
