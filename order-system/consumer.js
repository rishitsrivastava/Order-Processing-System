import { Kafka } from "kafkajs";
import pool from "./db.js";

const kafka = new Kafka({
  clientId: "order-consumer",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({ groupId: "order-group" });

const run = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: "orders", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const order = JSON.parse(message.value.toString());

        console.log("Received event: ", order);

        const { event, order_id, user_id, product_id } = order;

        try {
          switch (event) {
            case "ORDER_BOOKED":
              await pool.query(
                `INSERT INTO orders (order_id, user_id, product_id, status, created_at) VALUES ($1, $2, $3, $4, NOW()) ON CONFLICT (order_id) DO UPDATE SET STATUS = EXCLUDED.status`,
                [order_id, user_id, product_id, "BOOKED"]
              );
              console.log(`Order ${order_id} booked`);
              break;

            case "ORDER_SHIPPED":
              await pool.query(
                "UPDATE orders SET status = $1 WHERE order_id = $2",
                ["SHIPPED", order_id]
              );
              console.log(`Order ${order_id} shipped.`);
              break;

            case "ORDER_DELIVERED":
              await pool.query(
                "UPDATE orders SET status = $1 where order_id = $2",
                ["DELIVERED", order_id]
              );
              console.log(`Order ${order_id} delivered`);
              break;

            case "ORDER_CANCELLED":
              await pool.query(
                "UPDATE orders SET status = $1 WHERE order_id = $2",
                ["CANCELLED", order_id]
              );
              console.log(`Order ${order_id} cancelled`);
              break;

            default:
              console.log("Unknown event: ", event);
          }
        } catch (err) {
          console.error("Error processing the event: ", err);
        }
      } catch (err) {
        console.error("Error in processing the message:", err);
      }
    },
  });
};

run().catch(console.error);