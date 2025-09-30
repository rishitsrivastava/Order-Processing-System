import { Kafka } from "kafkajs";

const kafka = new Kafka({
    clientId: "my-app",
    brokers: ["localhost:9092"],
});

const producer = kafka.producer();

const run = async () => {
    await producer.connect();

    const order = {
        order_id: "123",
        user_id: "ris",
        product_id: "LP533",
        status: "PLACED"
    }

    await producer.send({
        topic: "orders",
        messages: [{ value: JSON.stringify(order)}],
    });

    console.log("Message sent to Kafka"),
        await producer.disconnect();
};

run().catch(console.error);