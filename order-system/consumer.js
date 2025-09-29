import { Kafka } from "kafkajs";
import pool from "./db.js";
import pkg from "pg";

const kafka = new Kafka({
  clientId: "my-app",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({ groupId: "test-group" });



const run = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: "orders", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const order = JSON.parse(message.value.toString)
        const orderID = message.key?.toString() || "unknown";
        const value = message.value.toString();

        console.log(`Received: ${order}`);

        const query = `
          INSERT INTO orders (order_id, user_id, product_id, status)
          VALUES ($1, $2, $3, $4)
          ON CONFLICT (order_id) DO NOTHING
        `;
      } catch (err) {
        console.log("error: ", err);
      }
    },
  });
};

run().catch(console.error);
