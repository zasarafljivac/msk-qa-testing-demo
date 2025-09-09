import assert from 'node:assert/strict';
import { execSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { createTopicIfMissing, publishOrder } from '../src/producer.mjs';
import { startConsumer, stopConsumer } from '../src/consumer.mjs';
import { getOrderById, getPool } from '../src/db.mjs';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const waitFor = async (fn, { tries = 120, interval = 500 } = {}) => {
  let lastErr;
  for (let i = 0; i < tries; i++) {
    try {
      const v = await fn();
      if (v) return v;
    } catch (e) {
      lastErr = e;
    }
    await sleep(interval);
  }
  if (lastErr) throw lastErr;
  throw new Error('waitFor timed out');
};

function approxEqual(a, b, digits = 2) {
  const factor = 10 ** digits;
  return Math.round(a * factor) === Math.round(b * factor);
}

async function beforeAll() {
  // Give containers a moment to be ready
  await sleep(2000);
  await createTopicIfMissing();
  await startConsumer();
}

async function afterAll() {
  await stopConsumer();
  await getPool().end();
}

async function runTest(name, fn) {
  const t0 = Date.now();
  process.stdout.write(`• ${name} ... `);
  try {
    await fn();
    console.log(`OK (${Date.now() - t0} ms)`);
    return { name, ok: true };
  } catch (err) {
    console.log(`FAIL (${Date.now() - t0} ms)\n  -> ${err?.stack ?? err}`);
    return { name, ok: false, err };
  }
}

// Happy path — order appears in DB with expected fields */
async function happyPath() {
  const orderId = randomUUID();
  const order = {
    orderId,
    partnerId: 'p-1001',
    status: 'PLACED',
    totalAmount: 25.99,
    currency: 'USD',
    isDeleted: false,
  };

  await publishOrder(order);

  const row = await waitFor(() => getOrderById(orderId));
  assert.ok(row, 'row should exist');
  assert.equal(row.orderId, orderId);
  assert.equal(row.partnerId, 'p-1001');
  assert.equal(row.status, 'PLACED');
  assert.ok(approxEqual(parseFloat(row.totalAmount), 25.99, 2), 'amount ~ 25.99');
  assert.equal(row.currency, 'USD');
  assert.equal(row.isDeleted, false);
  assert.ok(new Date(row.createdTs).getTime() > 0, 'created_ts set');
  assert.ok(new Date(row.updatedTs).getTime() > 0, 'updated_ts set');
}

/** Idempotency — duplicate publish updates same row */
async function idempotency() {
  const orderId = randomUUID();
  const base = {
    orderId,
    partnerId: 'p-dup',
    status: 'PLACED',
    totalAmount: 10.0,
    currency: 'USD',
    isDeleted: false,
  };

  await publishOrder(base);
  const first = await waitFor(() => getOrderById(orderId));

  await publishOrder(base);
  const second = await waitFor(async () => {
    const r = await getOrderById(orderId);
    return new Date(r.updatedTs).getTime() > new Date(first.updatedTs).getTime() ? r : null;
  });

  assert.equal(second.orderId, orderId);
  assert.equal(second.partnerId, 'p-dup');
  assert.equal(second.status, 'PLACED');
  assert.ok(approxEqual(parseFloat(second.totalAmount), 10.0, 2), 'amount ~ 10.0');
  assert.equal(
    new Date(second.createdTs).getTime(),
    new Date(first.createdTs).getTime(),
    'created_ts should remain constant'
  );
  assert.ok(
    new Date(second.updatedTs).getTime() > new Date(first.updatedTs).getTime(),
    'updated_ts should advance'
  );
}

/** Latest status wins — PLACED -> CAPTURED */
async function latest() {
  const orderId = randomUUID();

  await publishOrder({
    orderId,
    partnerId: 'p-2001',
    status: 'PLACED',
    totalAmount: 123.45,
    currency: 'USD',
    isDeleted: false,
  });

  await waitFor(() => getOrderById(orderId));

  await publishOrder({
    orderId,
    partnerId: 'p-2001',
    status: 'CAPTURED',
    totalAmount: 123.45,
    currency: 'USD',
    isDeleted: false,
  });

  const finalRow = await waitFor(async () => {
    const r = await getOrderById(orderId);
    return r && r.status === 'CAPTURED' ? r : null;
  });

  assert.equal(finalRow.status, 'CAPTURED');
  assert.ok(
    new Date(finalRow.updatedTs).getTime() > new Date(finalRow.createdTs).getTime(),
    'updated_ts should be after created_ts'
  );
}

/** Retry after outage — DB down then up, no data lost */
async function retryAfterOutage() {
  const orderId = randomUUID();

  // Stop MySQL to force consumer DB write failures
  execSync('docker compose stop mysql', { stdio: 'inherit' });

  await publishOrder({
    orderId,
    partnerId: 'p-outage',
    status: 'PLACED',
    totalAmount: 55.55,
    currency: 'USD',
    isDeleted: false,
  });

  // Allow consumer to attempt & retry while DB is down
  await sleep(4000);

  // Bring MySQL back
  execSync('docker compose start mysql', { stdio: 'inherit' });

  // The consumer should retry and eventually succeed
  const row = await waitFor(async () => {
    try {
      return await getOrderById(orderId);
    } catch {
      return null;
    }
  }, { tries: 180, interval: 1000 });

  assert.ok(row, 'row should exist after DB comeback');
  assert.equal(row.orderId, orderId);
  assert.equal(row.partnerId, 'p-outage');
  assert.equal(row.status, 'PLACED');
}

async function main() {
  const results = [];
  let hadFailure = false;

  console.log('== BeforeAll ==');
  await beforeAll();

  try {
    results.push(await runTest('Happy path', happyPath));
    results.push(await runTest('Idempotency', idempotency));
    results.push(await runTest('Latest status wins', latest));
    results.push(await runTest('Retry after outage', retryAfterOutage));

    hadFailure = results.some(r => !r.ok);
  } finally {
    console.log('== AfterAll ==');
    await afterAll();
  }

  console.log('\nSummary:');
  for (const r of results) {
    console.log(`${r.ok ? '✅' : '❌'} ${r.name}`);
  }

  process.exit(hadFailure ? 1 : 0);
}

main().catch(err => {
  console.error('Fatal runner error:', err);
  process.exit(1);
});
