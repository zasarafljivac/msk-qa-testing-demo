import mysql from 'mysql2/promise';

let pool = null;

function getPool() {
  if (!pool) {
    pool = mysql.createPool({
      host: process.env.DB_HOST || '127.0.0.1',
      user: process.env.DB_USER || 'app',
      password: process.env.DB_PASSWORD || 'app',
      database: process.env.DB_NAME || 'ops',
      port: process.env.PORT || 3309,
      waitForConnections: true,
      connectionLimit: 5,
    });
  }
  return pool;
}

function parse(uuid) {
  const hex = uuid.replace(/-/g, '').toLowerCase();
  if (!/^[0-9a-f]{32}$/.test(hex)) {
    throw new Error(`Invalid UUID: ${uuid}`);
  }
  return Buffer.from(hex, 'hex');
}

function stringify(buf) {
  const hex = buf.toString('hex');
  return (
    hex.slice(0, 8) + '-' +
    hex.slice(8, 12) + '-' +
    hex.slice(12, 16) + '-' +
    hex.slice(16, 20) + '-' +
    hex.slice(20)
  );
}

/**
 * Upsert an order into ops.orders_ops.
 * Fields:
 *   orderId (uuid string), partnerId, status, totalAmount (number/string), currency (default 'USD'), isDeleted (boolean)
 * created_ts is handled by DB default; updated_ts is bumped on update.
 */

async function upsertOrder({ orderId, partnerId, status, totalAmount, currency = 'USD', isDeleted = false }) {
  const sql = `
    INSERT INTO orders_ops (order_id, partner_id, status, total_amount, currency, is_deleted)
    VALUES (?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      partner_id = VALUES(partner_id),
      status = VALUES(status),
      total_amount = VALUES(total_amount),
      currency = VALUES(currency),
      is_deleted = VALUES(is_deleted),
      updated_ts = CURRENT_TIMESTAMP(3)
  `;

  const params = [
    parse(orderId),
    partnerId,
    status,
    totalAmount,
    currency,
    isDeleted ? 1 : 0,
  ];
  const [res] = await getPool().query(sql, params);
  return res;
}

/** Get a row by orderId (uuid string). Returns a normalized JS object or null. */
async function getOrderById(orderId) {
  const [rows] = await getPool().query(
    `SELECT order_id, partner_id, status, total_amount, currency, created_ts, updated_ts, is_deleted
     FROM orders_ops WHERE order_id = ?`,
    [ parse(orderId) ] 
  );
  if (rows.length === 0) {
    return null;
  }
  const r = rows[0];
  return {
    orderId: stringify(r.order_id),
    partnerId: r.partner_id,
    status: r.status,
    totalAmount: r.total_amount,
    currency: r.currency,
    createdTs: r.created_ts,
    updatedTs: r.updated_ts,
    isDeleted: !!r.is_deleted,
  };
}

async function listByPartner(partnerId) {
  const [rows] = await getPool().query(
    `SELECT order_id, partner_id, status, total_amount, currency, created_ts, updated_ts, is_deleted
     FROM orders_ops WHERE partner_id = ? ORDER BY created_ts DESC`,
    [partnerId]
  );
  return rows.map(r => ({
    orderId: stringify(r.order_id),
    partnerId: r.partner_id,
    status: r.status,
    totalAmount: r.total_amount,
    currency: r.currency,
    createdTs: r.created_ts,
    updatedTs: r.updated_ts,
    isDeleted: !!r.is_deleted,
  }));
}

export { getPool, upsertOrder, getOrderById, listByPartner, parse, stringify };