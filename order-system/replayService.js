import { Kafka } from "kafkajs";

const kafka = new Kafka({
    clientId: "replay-service",
    brokers: ["localhost:9092"] 
})

const consumer = kafka.consumer({ groupId: "replay-group" });
const producer = kafka.producer();

async function replayDLQ() {
    await consumer.connect();
    await producer.connect();

    await consumer.subscribe({ topic: "orders.DLQ", fromBeginning: true });
    console.log("Replay service connected to orders.DLQ");

    let replayCount = 0;

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            try {
                const dlqEvent = JSON.parse(message.value.toString());
                console.log(`Replaying event ${dlqEvent.eventId} for ordder ${dlqEvent.orderId}`);

                await producer.send({
                  topic: "orders.main",
                  messages: [
                    {
                          key: dlqEvent.eventId,
                        value: JSON.stringify(dlqEvent.payload),
                    },
                  ],
                });
            } catch (err) {
                console.log("Error replaying message:", err.messaage);
            }
        }
    })
}

replayDLQ().catch((err) => {
    console.log("Replay service failed: ", err)
})