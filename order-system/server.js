import express from "express";
import bodyParser from "body-parser";
import { Kafka, Partitioners } from "kafkajs";
import pool from "./db.js";

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

//>>>>>>>>>>>>>>>>>Get all Orders<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
app.get("/orders", async (req, res) => {
    try{
        const { id } = req.params;
        const result = await pool.query("SELECT * FROM orders ORDER BY created_at DESC");
        res.json(result.rows);
    } catch(err) {
        console.log("error in fetching orders from DB: ", err);
        res.status(400).json({ error: "Internal Server Error"});
    }
});


//>>>>>>>>>>>>>>>>>Create New Orders<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
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