import express from "express";
import bodyParser from "body-parser";
import { Kafka, Partitioners } from "kafkajs";

const app = express();
const port = 3000;

app.use(bodyParser.json()); 

const kafka = new Kafka({
    clientId: "order-api",
    brokers: ["localhost:9092"]
});

const producer = kafka.producer({
    createPartitioner: Partitioners.LegacyPartitioner
});

const initProducer = async () => {
    await producer.connect();
    console.log("Kafka Producer connected");
}

initProducer();

app.post("/orders", async (req, res) => {
    try {
        const { order_id, user_id, product_id, status } = req.body;

        if (!order_id || !user_id || !product_id || !status) {
            return res.status(400).json({error: "Missing required field"})
        }
        console.log("till here")
        await producer.send({
            topic: "orders",
            messages: [
                {
                    value: JSON.stringify({
                        order_id,
                        user_id,
                        product_id,
                        status,
                    }),
                },
            ],
        });
        res.status(201).json({ message: "Order sent to Kafka" });
    } catch (err) {
        res.status(500).json({ error: "Internal Server Error" });
    }
})

app.listen(port, () => {
    console.log(`order API running at http://localhost:${port}`);
});