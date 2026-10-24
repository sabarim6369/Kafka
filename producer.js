
const express = require("express");
const { Kafka } = require("kafkajs");

const app = express();
app.use(express.json());

const kafka = new Kafka({
  clientId: "order-service",
  brokers: ["localhost:29092", "localhost:39092"],
});

const producer = kafka.producer();

app.post("/orders", async (req, res) => {
  try {
    const { product, quantity } = req.body;

    // Create the event
    const order = {
      orderId: `order-${Date.now()}`,
      product,
      quantity,
      status: "CREATED",
      createdAt: new Date().toISOString(),
    };

    // Publish the event to Kafka
    await producer.send({
      topic: "orders",
      messages: [
        {
          key: order.orderId,
          value: JSON.stringify(order),
        },
      ],
    });

    // Respond after Kafka acknowledges the send
    res.status(201).json({
      message: "Order created and event published",
      order,
    });
  } catch (error) {
    console.error("Failed to publish order:", error);
    res.status(500).json({ error: "Failed to publish order event" });
  }
});

async function start() {
  await producer.connect();

  app.listen(3000, () => {
    console.log("Order service running on port 3000");
  });
}

start().catch(console.error);
