import { Kafka } from "kafkajs";
import pool from "../db.js";

const kafka = new Kafka({
  clientId: "shipping-consumer",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({ groupId: "shipping-group" });
const producer = kafka.producer();

const run = async () => {
  await consumer.connect();
  await producer.connect();
  await consumer.subscribe({ topic: "orders", fromBeginning: true });

  console.log("Shipping consumer is running...");

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const eventData = JSON.parse(message.value.toString());
      const { event, order_id, user_id, product_id } = eventData;

      try {
        switch (event) {
          case "ORDER_SHIPPED":
            await pool.query(
              "UPDATE orders SET status = $1 WHERE order_id = $2",
              ["SHIPPED", order_id]
            );
            console.log(`Order ${order_id} marked as shipped in DB.`);
            await producer.send({
              topic: "orders",
              messages: [
                {
                  value: JSON.stringify({
                    event: "ORDER_DELIVERED",
                    order_id,
                    user_id,
                    product_id,
                    status: "DELIVERED",
                  }),
                },
              ],
            });
            console.log(`📨 Emitted ORDER_DELIVERED for ${order_id}`);

            break;
        }
      } catch (error) {
        console.error("Error processing message from shippingConsumer:", error);
      }
    },
  });
};

if (process.argv[1].includes("shippingConsumer.js")) {
  run().catch(console.error);
}

export default run;
