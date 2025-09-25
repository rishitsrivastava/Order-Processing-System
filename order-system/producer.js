import { Kafka } from "kafkajs";

const kafka = new Kafka({
    clientId: "my-app",
    brokers: ["localhost:9092"],
});

const producer = kafka.producer();

const run = async () => {
    await producer.connect();
    await producer.send({
        topic: "orders",
        messages: [{ value: "Order placed for product P123 by user U456" }],
    });

    console.log("Message sent to Kafka"),
        await producer.disconnect();
};

run().catch(console.error);