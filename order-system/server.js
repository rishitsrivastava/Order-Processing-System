import express from "express";
import bodyParser from "body-parser";
import { Kafka, Partitioners } from "kafkajs";
import pool from "./db.js";
import { randomUUID } from "crypto";

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

//>>>>>>>>>>>>>>>>>Create New Orders (Booked)<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
app.post("/orders", async (req, res) => {
  try {
    const { order_id, user_id, product_id } = req.body;

    if (!order_id || !user_id || !product_id) {
      return res.status(400).json({ error: "Missing required field" });
    }
    await producer.send({
      topic: "orders.main",
      messages: [
        {
          value: JSON.stringify({
            event_id: randomUUID(),
            event: "ORDER_BOOKED",
            order_id,
            user_id,
            product_id,
          }),
        },
      ],
    });
    res.status(201).json({ message: "Order booked event sent to Kafka" });
  } catch (err) {
    res.status(500).json({ error: "Internal Server Error" });
  }
});

// >>>>>>>>>>>>>>>>>> Ship Order <<<<<<<<<<<<<<<<<<<<<<<
app.put("/orders/:id/ship", async (req, res) => {
  try {
    const {id} = req.params;

    await producer.send({
      topic: "orders.main",
      messages: [
        {
          value: JSON.stringify({
            event_id: randomUUID(),
            event: "ORDER_SHIPPED",
            order_id: id,
          }),
        },
      ],
    });

    res.json({ message: `Order ${id} shipped event sent`});
  } catch( err ) {
    console.error("error shipping order: ", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
})

// >>>>>>>>>>>>>>>>>> Deliver Order <<<<<<<<<<<<<<<<<<<<<<<
app.put("/orders/:id/deliver", async (req, res) => {
  try {
    const { id } = req.params;

    await producer.send({
      topic: "orders.main",
      messages: [
        {
          value: JSON.stringify({
            event_id: randomUUID(),
            event: "ORDER_DELIVERED",
            order_id: id,
          }),
        },
      ],
    });

    res.json({ message: `Order ${id} delivered event sent` });
  } catch (err) {
    console.error("Error delivering order:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

// >>>>>>>>>>>>>>>>>> Cancel Order <<<<<<<<<<<<<<<<<<<<<<<
app.put("/orders/:id/cancel", async (req, res) => {
  try {
    const { id } = req.params;

    await producer.send({
      topic: "orders.main",
      messages: [
        {
          value: JSON.stringify({
            event_id: randomUUID(),
            event: "ORDER_CANCELLED",
            order_id: id,
          }),
        },
      ],
    });
    res.json({ message: `Order ${id} cancelled event sent` });
  } catch (err) {
    console.error("Error cancelling order:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

//>>>>>>>>>>>>>>>>>Get all Orders<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
app.get("/orders", async (req, res) => {
  try {
    const result = await pool.query(
      "SELECT * FROM orders ORDER BY created_at DESC"
    );
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
    console.log("error fetching order by id: ", error);
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

//>>>>>>>>>>>>>>>>>>>Delete orders<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
app.delete("/orders/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const query = "DELETE FROM orders WHERE order_id=$1 RETURNING *";
    const result = await pool.query(query, [id]);
    if (result.rows.length === 0) {
      return res
        .status(400)
        .json({ error: `order not found from order_id: ${id}` });
    }
    res.json({ message: "order deleted ", order: result.rows[0] });
  } catch (err) {
    console.error("Error deleting order:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

//>>>>>>>>>>>>>>>>>>>get events from order_events_audit<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
app.get("/audit/:orderId", async (req, res) => {
  try {
    const { orderId } = req.params;
    const result = await pool.query(
      "SELECT * FROM order_events_audit WHERE order_id = $1 ORDER BY processed_at ASC",
      [orderId]
    );
    res.json(result.rows);
  } catch (err) {
    console.error("Error fetching audit trail:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
});


app.listen(port, () => {
  console.log(`order API running at http://localhost:${port}`);
});
