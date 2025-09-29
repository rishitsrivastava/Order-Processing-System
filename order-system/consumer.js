import { Kafka } from "kafkajs";
import pool from "./db.js";

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
      const orderID = message.key?.toString() || "unknown";
      const value = message.value.toString();

      console.log(`Received: ${value}`);

      const query = pool.query(
        "Insert INTO orders(order_id, status) VALUES($1, $2) ON CONFLICT DO NOTHING",
        [orderID, value]
      );
    },
  });
};

run().catch(console.error);
