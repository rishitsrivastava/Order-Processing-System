import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "notification-consumer",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({ groupId: "notification-group" });

const run = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: "orders", fromBeginning: true });

  console.log("🔔 NotificationConsumer is running...");

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const order = JSON.parse(message.value.toString());
      const { event, order_id, user_id, product_id } = order;

      switch (event) {
        case "ORDER_DELIVERED":
          console.log(
            `Notification: Order ${order_id} delivered to user ${user_id}`
          );
          break;

        case "ORDER_CANCELLED":
          console.log(
            `Notification: Order ${order_id} was cancelled for user ${user_id}`
          );
          break;

        default:
          // ignore other events
          break;
      }
    },
  });
};

if (process.argv[1].includes("notificationConsumer.js")) {
  run().catch(console.error);
}
