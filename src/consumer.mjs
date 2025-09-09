import { Kafka, logLevel } from 'kafkajs';
import pRetry from 'p-retry';
import  { upsertOrder } from './db.mjs';

const brokers = [process.env.KAFKA_BROKER || 'localhost:19092'];
const topic = process.env.KAFKA_TOPIC || 'orders.transformed';
const groupId = process.env.KAFKA_GROUP_ID || 'write-lambda-e2e';

const kafka = new Kafka({
  clientId: 'write-lambda',
  brokers,
  logLevel: logLevel.WARN,
});

let consumer;

/** which DB errors are worth retrying */
function isRetryable(err) {
  // Common transient conditions (network/db)
  const retryableCodes = new Set([
    'PROTOCOL_CONNECTION_LOST',
    'ECONNREFUSED',
    'ETIMEDOUT',
    'EPIPE',
    'ECONNRESET',
    'PROTOCOL_ENQUEUE_AFTER_FATAL_ERROR',
  ]);

  // MySQL errno: 1205 lock wait timeout, 1213 deadlock
  const retryableErrnos = new Set([1205, 1213]);

  return retryableCodes.has(err?.code) || retryableErrnos.has(err?.errno);
}

async function startConsumer() {
  consumer = kafka.consumer({ groupId });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const payload = JSON.parse(message.value.toString('utf8'));
      const order = {
        orderId: payload.orderId,
        partnerId: payload.partnerId,
        status: payload.status,
        totalAmount: payload.totalAmount,
        currency: payload.currency ?? 'USD',
        isDeleted: !!payload.isDeleted,
      };

      await pRetry(
        async () => {
          try {
            await upsertOrder(order);
          } catch (err) {
            if (!isRetryable(err)) {
              // no retry on non-transient errors
              throw new pRetry.AbortError(err);
            }
            throw err; // if retryable then backoff
          }
        },
        {
          // two retries with ~2s then ~4s waits
          retries: 2, // attempts = 1 initial + 2 retries = 3 total
          factor: 2, // exponential backoff factor
          minTimeout: 2000,
          maxTimeout: 5000,
          randomize: false, // set true to add jitter and reduce herd effects
          onFailedAttempt: (error) => {
            console.warn(
              `upsertOrder failed (attempt ${error.attemptNumber}, retries left: ${error.retriesLeft}): ${JSON.stringify(error)}`
            );
          },
        }
      );
      // If we get here, the write succeeded; kafkajs commits the offset after handler returns
    },
  });

  return consumer;
}

async function stopConsumer() {
  if (consumer) {
    await consumer.stop();
    await consumer.disconnect();
    consumer = null;
  }
}


export { startConsumer, stopConsumer };