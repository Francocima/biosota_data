// orders_ingestion.js
// Shopify Bulk Operation NDJSON Parser -> Postgres (orders.raw_orders_shopify)

const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');

// ============================================
// CONFIGURATION
// ============================================

const CONFIG = {
  localJsonlFile: path.join(__dirname, 'orders_raw_data.json'),
  useLocalFile: true,
  batchSize: 50,
  batchCooldownMs: 300,
  batchRetries: 3,
  testingLimit: 0, // set >0 to limit number of orders processed (for testing)
  database: {
    host: 'aws-1-ap-southeast-2.pooler.supabase.com',
    port: 5432,
    database: 'postgres',
    user: 'postgres.ilodhajofowqorjmlqtr',
    password: 'NCfk2AhvpCSz5KuF'
  }
};

// ============================================
// HELPERS
// ============================================

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function extractShopifyId(gid) {
  if (!gid) return null;
  const m = String(gid).match(/\/(\d+)(\?|$)/);
  return m ? Number(m[1]) : null;
}

// Try to extract order_number from name like "#1001" or "1001"
function extractOrderNumber(name) {
  if (!name) return null;
  const m = String(name).match(/#?(\d+)/);
  return m ? Number(m[1]) : null;
}

function safeGet(obj, pathArr) {
  return pathArr.reduce((acc, k) => (acc && acc[k] != null ? acc[k] : null), obj);
}

// Aggregate latest fulfillment status from fulfillments array
function latestFulfillment(fulfillments) {
  if (!Array.isArray(fulfillments) || fulfillments.length === 0) return { status: null, displayStatus: null };
  // choose last by createdAt if available, else last element
  const withDate = fulfillments.filter(f => !!f && !!f.createdAt);
  let latest;
  if (withDate.length > 0) {
    latest = withDate.reduce((a, b) => (new Date(a.createdAt) > new Date(b.createdAt) ? a : b));
  } else {
    latest = fulfillments[fulfillments.length - 1];
  }
  return { status: latest.status || null, displayStatus: latest.displayStatus || null };
}

// ============================================
// PARSE NDJSON / N8N-WRAPPED FILE
// ============================================

async function parseJsonlFile(filePath) {
  console.log('Reading Shopify NDJSON file...', filePath);
  if (!filePath || typeof filePath !== 'string') {
    throw new Error('Invalid file path provided to parseJsonlFile');
  }

  const raw = fs.readFileSync(filePath, 'utf8');
  let jsonlString = null;
  const trimmed = raw.trim();

  // support n8n wrapper { json: { data: "...." } } or array wrapper
  try {
    if (trimmed.startsWith('[') || trimmed.startsWith('{')) {
      const outer = JSON.parse(raw);
      if (Array.isArray(outer)) {
        const found = outer.find(el => el && el.json && typeof el.json.data === 'string');
        if (found) jsonlString = found.json.data;
        else jsonlString = outer.map(o => JSON.stringify(o)).join('\n');
      } else if (outer && outer.json && typeof outer.json.data === 'string') {
        jsonlString = outer.json.data;
      } else {
        jsonlString = JSON.stringify(outer);
      }
    }
  } catch (err) {
    // not JSON -> assume raw NDJSON below
  }

  if (jsonlString === null) jsonlString = raw;

  const lines = jsonlString
    .split('\n')
    .map(l => l.trim())
    .filter(l => l !== '');

  console.log(`✓ Found ${lines.length} lines`);

  const orders = [];
  let badLines = 0;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    let obj = null;
    try {
      obj = JSON.parse(line);
    } catch (err) {
      // try unwrapping quoted string (n8n oddities)
      try {
        if ((line.startsWith('"') && line.endsWith('"')) || (line.startsWith("'") && line.endsWith("'"))) {
          const unwrapped = line.slice(1, -1).replace(/\\n/g, '\n').replace(/\\"/g, '"').replace(/\\'/g, "'");
          obj = JSON.parse(unwrapped);
        } else {
          throw err;
        }
      } catch (err2) {
        console.warn(`⚠ Skipping invalid JSON line ${i + 1}: ${line.slice(0,200)}`);
        badLines++;
        continue;
      }
    }

    // ensure this is an Order object
    if (!obj || !obj.id) {
      // skip
      continue;
    }

    // accept both { __typename: "Order", ... } or plain order object
    orders.push(obj);

    if ((i + 1) % 1000 === 0) {
      process.stdout.write(`\rParsed lines: ${i + 1}/${lines.length}...`);
    }
  }

  if (badLines > 0) console.warn(`⚠ Skipped ${badLines} invalid lines`);
  console.log(`\n✓ Parsed ${orders.length} order objects`);

  return orders;
}

// ============================================
// TRANSFORM ORDER -> DB ROW
// ============================================

function transformOrder(order) {
  // addresses
  const billing = order.billingAddress || {};
  const shipping = order.shippingAddress || {};

  // totals - shopMoney wrappers exist under currentSubtotalPriceSet => shopMoney.amount
  const current_subtotal_price = safeGet(order, ['currentSubtotalPriceSet','shopMoney','amount']) || safeGet(order, ['currentSubtotalPrice','shopMoney','amount']) || null;
  const current_total_tax = safeGet(order, ['currentTotalTaxSet','shopMoney','amount']) || safeGet(order, ['currentTotalTax','shopMoney','amount']) || null;
  const current_total_discounts = safeGet(order, ['currentTotalDiscountsSet','shopMoney','amount']) || safeGet(order, ['currentTotalDiscounts','shopMoney','amount']) || null;
  const current_total_price = safeGet(order, ['currentTotalPriceSet','shopMoney','amount']) || safeGet(order, ['currentTotalPrice','shopMoney','amount']) || null;
  const total_shipping_price = safeGet(order, ['totalShippingPriceSet','shopMoney','amount']) || safeGet(order, ['currentTotalShippingPriceSet','shopMoney','amount']) || null;
  const total_refunded = safeGet(order, ['totalRefundedSet','shopMoney','amount']) || null;

  // fulfillment aggregated
  const f = latestFulfillment(order.fulfillments || []);

  // tags array -> CSV
  let tags = null;
  if (Array.isArray(order.tags)) {
    tags = order.tags.join(', ');
  } else if (typeof order.tags === 'string') {
    tags = order.tags;
  }

  return {
    order_id: extractShopifyId(order.id),
    customer_id: order.customer ? extractShopifyId(order.customer.id) : null,
    name: order.name || null,
    order_number: extractOrderNumber(order.name || order.orderNumber || null),
    created_at: order.createdAt || null,
    updated_at: order.updatedAt || null,
    cancelled_at: order.cancelledAt || null,
    cancel_reason: order.cancelReason || null,
    processed_at: order.processedAt || null,
    closed_at: order.closedAt || null,
    confirmed: typeof order.confirmed === 'boolean' ? order.confirmed : null,
    test: typeof order.test === 'boolean' ? order.test : null,
    tags: tags,
    note: order.note || null,
    source_name: order.sourceName || null,
    current_subtotal_price: current_subtotal_price ? Number(current_subtotal_price) : null,
    current_total_tax: current_total_tax ? Number(current_total_tax) : null,
    current_total_discounts: current_total_discounts ? Number(current_total_discounts) : null,
    current_total_price: current_total_price ? Number(current_total_price) : null,
    total_shipping_price: total_shipping_price ? Number(total_shipping_price) : null,
    total_refunded: total_refunded ? Number(total_refunded) : null,
    currency_code: order.currencyCode || null,

    billing_first_name: billing.firstName || null,
    billing_last_name: billing.lastName || null,
    billing_company: billing.company || null,
    billing_address1: billing.address1 || null,
    billing_address2: billing.address2 || null,
    billing_city: billing.city || null,
    billing_province: billing.province || null,
    billing_province_code: billing.provinceCode || null,
    billing_zip: billing.zip || null,
    billing_country: billing.country || null,
    billing_country_code: billing.countryCodeV2 || billing.countryCode || null,
    billing_phone: billing.phone || null,

    shipping_first_name: shipping.firstName || null,
    shipping_last_name: shipping.lastName || null,
    shipping_company: shipping.company || null,
    shipping_address1: shipping.address1 || null,
    shipping_address2: shipping.address2 || null,
    shipping_city: shipping.city || null,
    shipping_province: shipping.province || null,
    shipping_province_code: shipping.provinceCode || null,
    shipping_zip: shipping.zip || null,
    shipping_country: shipping.country || null,
    shipping_country_code: shipping.countryCodeV2 || shipping.countryCode || null,
    shipping_phone: shipping.phone || null,

    fulfillment_status: f.status,
    fulfillment_display_status: f.displayStatus
  };
}

// ============================================
// SAVE AS CSV/JSON (if needed)
// ============================================

function saveAsCSV(rows, filename = 'orders_export.csv') {
  if (!rows || rows.length === 0) {
    console.log('No rows to save');
    return;
  }
  const headers = Object.keys(rows[0]);
  let csv = headers.join(',') + '\n';
  rows.forEach(r => {
    const row = headers.map(h => {
      let v = r[h];
      if (v === null || v === undefined) return '';
      v = String(v).replace(/"/g, '""');
      if (v.includes(',') || v.includes('\n') || v.includes('"')) v = `"${v}"`;
      return v;
    });
    csv += row.join(',') + '\n';
  });
  fs.writeFileSync(filename, csv);
  console.log(`✓ Saved ${rows.length} rows to ${filename}`);
}

function saveAsJSON(rows, filename = 'orders_export.json') {
  fs.writeFileSync(filename, JSON.stringify(rows, null, 2));
  console.log(`✓ Saved ${rows.length} rows to ${filename}`);
}

// ============================================
// DATABASE POOL
// ============================================

const pool = new Pool({
  ...CONFIG.database,
  max: 3,
  idleTimeoutMillis: 10000,
  connectionTimeoutMillis: 5000
});

// ============================================
// Build upsert SQL for orders (batch)
// ============================================

function buildUpsertQueryBatch(rows) {
  // columns must exactly match transformOrder keys (and your table columns)
  const columns = [
    'order_id','customer_id','name','order_number','created_at','updated_at',
    'cancelled_at','cancel_reason','processed_at','closed_at','confirmed','test',
    'tags','note','source_name','current_subtotal_price','current_total_tax',
    'current_total_discounts','current_total_price','total_shipping_price',
    'total_refunded','currency_code',
    'billing_first_name','billing_last_name','billing_company','billing_address1','billing_address2','billing_city','billing_province','billing_province_code','billing_zip','billing_country','billing_country_code','billing_phone',
    'shipping_first_name','shipping_last_name','shipping_company','shipping_address1','shipping_address2','shipping_city','shipping_province','shipping_province_code','shipping_zip','shipping_country','shipping_country_code','shipping_phone',
    'fulfillment_status','fulfillment_display_status'
  ];

  const values = [];
  const valuePlaceholders = rows.map((r, rowIndex) => {
    const placeholders = columns.map((_, colIndex) => {
      const idx = rowIndex * columns.length + colIndex + 1;
      return `$${idx}`;
    });
    // push values
    values.push(
      r.order_id, r.customer_id, r.name, r.order_number, r.created_at, r.updated_at,
      r.cancelled_at, r.cancel_reason, r.processed_at, r.closed_at, r.confirmed, r.test,
      r.tags, r.note, r.source_name, r.current_subtotal_price, r.current_total_tax,
      r.current_total_discounts, r.current_total_price, r.total_shipping_price,
      r.total_refunded, r.currency_code,
      r.billing_first_name, r.billing_last_name, r.billing_company, r.billing_address1, r.billing_address2, r.billing_city, r.billing_province, r.billing_province_code, r.billing_zip, r.billing_country, r.billing_country_code, r.billing_phone,
      r.shipping_first_name, r.shipping_last_name, r.shipping_company, r.shipping_address1, r.shipping_address2, r.shipping_city, r.shipping_province, r.shipping_province_code, r.shipping_zip, r.shipping_country, r.shipping_country_code, r.shipping_phone,
      r.fulfillment_status, r.fulfillment_display_status
    );
    return `(${placeholders.join(',')})`;
  });

  const insertSection = `INSERT INTO orders.raw_orders_shopify (${columns.join(', ')}) VALUES ${valuePlaceholders.join(', ')}`;

  const updateSets = columns
    .filter(c => c !== 'order_id')
    .map(c => `${c} = EXCLUDED.${c}`)
    .join(', ');

  const text = `${insertSection} ON CONFLICT (order_id) DO UPDATE SET ${updateSets}`;
  return { text, values };
}

async function insertBatch(rows) {
  if (!rows || rows.length === 0) return { success: 0, failed: 0 };
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const { text, values } = buildUpsertQueryBatch(rows);
    await client.query(text, values);
    await client.query('COMMIT');
    return { success: rows.length, failed: 0 };
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

// ============================================
// BATCH PROCESSING WITH RETRIES
// ============================================

async function processBatch(batchRows, batchNum, totalBatches, outputFormat = 'database') {
  console.log(`\nProcessing batch ${batchNum}/${totalBatches} (${batchRows.length} rows)`);

  if (outputFormat !== 'database') {
    if (outputFormat === 'csv') saveAsCSV(batchRows, `orders_batch_${batchNum}.csv`);
    else saveAsJSON(batchRows, `orders_batch_${batchNum}.json`);
    return;
  }

  let attempt = 0;
  while (attempt < CONFIG.batchRetries) {
    try {
      const result = await insertBatch(batchRows);
      console.log(`✓ Batch ${batchNum}: inserted ${result.success} rows`);
      break;
    } catch (err) {
      attempt++;
      console.error(`⚠ Error inserting batch ${batchNum} (attempt ${attempt}):`, err.message || err);
      if (attempt >= CONFIG.batchRetries) {
        console.error(`✖ Batch ${batchNum} failed after ${attempt} attempts`);
        throw err;
      } else {
        const backoff = 250 * attempt;
        console.log(`→ Retrying after ${backoff}ms`);
        await sleep(backoff);
      }
    }
  }

  await sleep(CONFIG.batchCooldownMs);
  console.log(`✓ Batch ${batchNum} complete`);
}

// ============================================
// MAIN
// ============================================

async function main() {
  try {
    console.log('🚀 Starting Shopify Orders Import\n');
    console.log('DEBUG path:', CONFIG.localJsonlFile);

    const ordersRaw = await parseJsonlFile(CONFIG.localJsonlFile);
    if (!ordersRaw || ordersRaw.length === 0) {
      console.log('No orders to process - exiting.');
      return;
    }

    // transform all orders to DB rows
    const allRows = ordersRaw.map(transformOrder);

    // limit to testing set if configured
    if (CONFIG.testingLimit && CONFIG.testingLimit > 0) {
      allRows.splice(CONFIG.testingLimit);
    }

    // choose output method
    const OUTPUT = 'database'; // 'csv' | 'json' | 'database'
    if (OUTPUT === 'csv') {
      saveAsCSV(allRows, 'orders_export_all.csv');
      return;
    } else if (OUTPUT === 'json') {
      saveAsJSON(allRows, 'orders_export_all.json');
      return;
    }

    // chunk into batches
    const batches = [];
    for (let i = 0; i < allRows.length; i += CONFIG.batchSize) {
      batches.push(allRows.slice(i, i + CONFIG.batchSize));
    }

    console.log(`Processing ${batches.length} batches of up to ${CONFIG.batchSize} rows\n`);
    for (let i = 0; i < batches.length; i++) {
      await processBatch(batches[i], i + 1, batches.length, 'database');
    }

    console.log('\n✅ All orders processed successfully');
  } catch (err) {
    console.error('\n❌ Fatal error:', err);
    process.exit(1);
  } finally {
    try {
      await pool.end();
    } catch (e) {}
  }
}

main();
