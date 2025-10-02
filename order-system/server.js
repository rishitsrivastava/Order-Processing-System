import express from "express";
import bodyParser from "body-parser";
import { Kafka, Partitioners } from "kafkajs";
import pool from "./db.js";

const app = express();
const port = 3000;

app.use(bodyParser.json());

const kafka = new Kafka({
  clientId: "order-api",
  brokers: ["localhost:9092"],
});

const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner,
});

const initProducer = async () => {
  await producer.connect();
  console.log("Kafka Producer connected");
};

initProducer();

//>>>>>>>>>>>>>>>>>Get all Orders<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
app.get("/orders", async (req, res) => {
  try {
    const result = await pool.query("SELECT * FROM orders BY created_at DESC");
    res.json(result.rows);
  } catch (err) {
    console.log("error in fetching orders from DB: ", err);
    res.status(400).json({ error: "Internal Server Error" });
  }
});

//>>>>>>>>>>>>>>>>>Get a order from order ID<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
app.get("/orders/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const result = await pool.query(
      "SELECT * FROM orders WHERE order_id = $1",
      [id]
    );
    if (result.rows.length === 0) {
      return res.status(404).json({ error: "Order not found" });
    }
    res.json(result.rows[0]);
  } catch (error) {
    console.log("error fetching order by id: ", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

//>>>>>>>>>>>>>>>>>update order status<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
app.put("/orders/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const { status } = req.body;

    if (!status) return res.status(400).json({ error: "status is required" });
    const query =
      "UPDATE orders SET status = $1 WHERE order_id = $2 RETURNING *";
    const result = await pool.query(query, [status, id]);
    if (result.rows.length === 0) {
      return res.status(400).json({ error: "order not found" });
    }
    res.json({ message: "Order status updated", order: result.rows[0] });
  } catch (err) {
    console.error("Error updating order status:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

//>>>>>>>>>>>>>>>>>Create New Orders<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
app.post("/orders", async (req, res) => {
  try {
    const { order_id, user_id, product_id, status } = req.body;

    if (!order_id || !user_id || !product_id || !status) {
      return res.status(400).json({ error: "Missing required field" });
    }
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
});

app.listen(port, () => {
  console.log(`order API running at http://localhost:${port}`);
});
