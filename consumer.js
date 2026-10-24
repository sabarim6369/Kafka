
const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "payment-service",
  brokers: ["localhost:29092", "localhost:39092"],
});

const consumer = kafka.consumer({
  groupId: "payment-service-group",
});

async function start() {
  await consumer.connect();

  await consumer.subscribe({
    topic: "orders",
    fromBeginning: false,
  });

  console.log("Payment service waiting for order events...");

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const order = JSON.parse(message.value.toString());

      console.log("Received event:", {
        topic,
        partition,
        offset: message.offset,
        order,
      });

      // Simulate payment processing
      console.log(`Processing payment for ${order.orderId}`);
    },
  });
}

start().catch(console.error);
