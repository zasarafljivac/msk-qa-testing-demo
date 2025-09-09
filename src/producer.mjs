import { Kafka, logLevel } from 'kafkajs';

const brokers = [process.env.KAFKA_BROKER || 'localhost:19092'];
const topic = process.env.KAFKA_TOPIC || 'orders.transformed';

const kafka = new Kafka({
  clientId: 'transform-lambda',
  brokers,
  logLevel: logLevel.WARN,
});

async function createTopicIfMissing() {
  try {
    const admin = kafka.admin();
    await admin.connect();
    const topics = await admin.listTopics();
    if (!topics.includes(topic)) {
      await admin.createTopics({
        topics: [{ topic, numPartitions: 1, replicationFactor: 1 }],
      });
    }
    await admin.disconnect();
    console.log(`Topic "${topic}" is ready`);

  } catch(err) {
    console.error('Error creating topic:', err);
  }
}
/**
 * Publish an order message shaped for orders_ops:
 * {
 *   orderId: 'uuid',
 *   partnerId: 'p-123',
 *   status: 'PLACED' | 'CAPTURED' | 'CANCELLED' | ... 'WHATEVER',
 *   totalAmount: 25.99,
 *   currency: 'USD',
 *   isDeleted: false
 * }
 */
async function publishOrder(order) {
  const msg = {
    orderId: order.orderId,
    partnerId: order.partnerId,
    status: order.status,
    totalAmount: order.totalAmount,
    currency: order.currency ?? 'USD',
    isDeleted: !!order.isDeleted,
  };

  const producer = kafka.producer();
  await producer.connect();
  await producer.send({
    topic,
    messages: [{ key: msg.orderId, value: JSON.stringify(msg) }],
  });
  await producer.disconnect();
}


export { createTopicIfMissing, publishOrder };