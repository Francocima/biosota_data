// Shopify Bulk Operation JSONL Parser (robust + safe DB ingestion)
// Paste this file over your existing customers_ingestion.js

const fs = require('fs');
const path = require('path');

// ============================================
// CONFIGURATION
// ============================================

const CONFIG = {
  // Always resolve relative to script so it works regardless of CWD
  localJsonlFile: path.join(__dirname, 'customers_raw_data.json'),

  // Use local file or download from URL?
  useLocalFile: true,

  // Batch size for database inserts (keep small for Supabase free/small tiers)
  batchSize: 20,

  // Pause (ms) between batches to avoid hitting DB limits
  batchCooldownMs: 300,

  // Retry attempts per batch
  batchRetries: 3,

  // Database configuration (adjust for your database)
  database: {
    host: 'aws-1-ap-southeast-2.pooler.supabase.com',
    port: 5432,
    database: 'postgres',
    user: 'postgres.ilodhajofowqorjmlqtr',
    password: 'NCfk2AhvpCSz5KuF'
  }
};

// ============================================
// Helpers
// ============================================

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function extractShopifyId(gid) {
  if (!gid) return null;
  const match = String(gid).match(/\/(\d+)(\?|$)/);
  return match ? match[1] : null;
}

// ============================================
// PARSE SHOPIFY JSONL / N8N-WRAPPED FILE
// ============================================

async function parseJsonlFile(filePath) {
  console.log('Reading Shopify JSONL file...');
  if (!filePath || typeof filePath !== 'string') {
    throw new Error('Invalid file path provided to parseJsonlFile');
  }

  const rawContent = fs.readFileSync(filePath, 'utf8');
  let jsonlString = null;
  const trimmed = rawContent.trim();

  try {
    if (trimmed.startsWith('[') || trimmed.startsWith('{')) {
      const outer = JSON.parse(rawContent);

      if (Array.isArray(outer)) {
        const found = outer.find(el => el && el.json && typeof el.json.data === 'string');
        if (found) jsonlString = found.json.data;
        else jsonlString = outer.map(o => JSON.stringify(o)).join('\n');
      } else if (outer && outer.json && typeof outer.json.data === 'string') {
        jsonlString = outer.json.data;
      } else {
        // fallback to stringifying the object as JSONL (rare)
        jsonlString = JSON.stringify(outer);
      }
    }
  } catch (err) {
    // If we couldn't parse outer JSON, assume rawContent is plain JSONL
  }

  if (jsonlString === null) jsonlString = rawContent;

  const lines = jsonlString.split('\n').map(l => l.trim()).filter(l => l !== '');
  console.log(`✓ Found ${lines.length} JSONL lines`);

  const customers = {};     // map gid => customer obj
  const pendingOrders = {}; // map parentGid => [order, ...]
  let orderCount = 0;
  let badLines = 0;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    let obj;
    try {
      obj = JSON.parse(line);
    } catch (err) {
      // Try unwrapping quoted JSON inside a string (n8n weirdness), otherwise skip
      try {
        if ((line.startsWith('"') && line.endsWith('"')) || (line.startsWith("'") && line.endsWith("'"))) {
          const unwrapped = line.slice(1, -1).replace(/\\n/g, '\n').replace(/\\"/g, '"').replace(/\\'/g, "'");
          obj = JSON.parse(unwrapped);
        } else {
          throw err;
        }
      } catch (err2) {
        console.warn(`⚠ Skipping invalid JSON line ${i + 1}:`, line.slice(0, 200));
        badLines++;
        continue;
      }
    }

    const id = obj && obj.id ? obj.id : null;
    if (!id) continue;

    if (id.includes('/Customer/')) {
      if (!obj.orders) obj.orders = [];
      if (pendingOrders[id]) {
        obj.orders = obj.orders.concat(pendingOrders[id]);
        delete pendingOrders[id];
      }
      customers[id] = obj;
    } else if (id.includes('/Order/') || id.includes('/DraftOrder/')) {
      const parentId = obj.__parentId || obj.__parent_id || obj.parentId || obj.parent;
      if (parentId) {
        orderCount++;
        if (customers[parentId]) {
          customers[parentId].orders = customers[parentId].orders || [];
          customers[parentId].orders.push(obj);
        } else {
          pendingOrders[parentId] = pendingOrders[parentId] || [];
          pendingOrders[parentId].push(obj);
        }
      } else {
        // order without parent - skip for now
      }
    }
    if ((i + 1) % 1000 === 0) {
      process.stdout.write(`\rParsed lines: ${i + 1}/${lines.length}...`);
    }
  }

  const pendingCount = Object.values(pendingOrders).reduce((acc, arr) => acc + arr.length, 0);
  if (pendingCount > 0) {
    console.warn(`⚠ ${pendingCount} orders could not be attached to a customer (customer records missing)`);
  }

  console.log(`\n✓ Parsed ${Object.keys(customers).length} customers`);
  console.log(`✓ Parsed ${orderCount} orders`);
  if (badLines > 0) console.warn(`⚠ Skipped ${badLines} invalid JSON lines`);

  return Object.values(customers);
}

// ============================================
// TRANSFORM CUSTOMER DATA FOR DATABASE
// ============================================

function transformCustomer(customer) {
  return {
    customer_id: extractShopifyId(customer.id),
    first_name: customer.firstName,
    last_name: customer.lastName,
    email: customer.email,
    display_name: customer.displayName,
    phone: customer.phone || customer.defaultAddress?.phone,

    address_company: customer.defaultAddress?.company,
    address_1: customer.defaultAddress?.address1,
    address_2: customer.defaultAddress?.address2,
    city: customer.defaultAddress?.city,
    state_code: customer.defaultAddress?.provinceCode,
    country_code: customer.defaultAddress?.countryCode,
    zip_code: customer.defaultAddress?.zip,

    note: customer.note,
    tax_exempt: customer.taxExempt,
    verified_email: customer.verifiedEmail,
    valid_email_address: customer.validEmailAddress,

    created_at: customer.createdAt,
    updated_at: customer.updatedAt,
    deleted_at: null,

    email_marketing_state: customer.emailMarketingConsent?.marketingState,
    email_marketing_opt_in_level: customer.emailMarketingConsent?.marketingOptInLevel,
    email_consent_updated_at: customer.emailMarketingConsent?.consentUpdatedAt,

    sms_marketing_state: customer.smsMarketingConsent?.marketingState,
    sms_marketing_opt_in_level: customer.smsMarketingConsent?.marketingOptInLevel,
    sms_consent_updated_at: customer.smsMarketingConsent?.consentUpdatedAt
  };
}

// ============================================
// SAVE AS CSV / JSON (unchanged)
// ============================================

function saveAsCSV(customers, filename = 'customers_export.csv') {
  const transformed = customers.map(transformCustomer);
  if (transformed.length === 0) {
    console.log('No customers to export');
    return;
  }
  const headers = Object.keys(transformed[0]);
  let csv = headers.join(',') + '\n';
  transformed.forEach(customer => {
    const row = headers.map(header => {
      let value = customer[header];
      if (value === null || value === undefined) return '';
      value = String(value).replace(/"/g, '""');
      if (value.includes(',') || value.includes('\n') || value.includes('"')) value = `"${value}"`;
      return value;
    });
    csv += row.join(',') + '\n';
  });
  fs.writeFileSync(filename, csv);
  console.log(`✓ Saved ${transformed.length} customers to ${filename}`);
}

function saveAsJSON(customers, filename = 'customers_export.json') {
  const transformed = customers.map(transformCustomer);
  fs.writeFileSync(filename, JSON.stringify(transformed, null, 2));
  console.log(`✓ Saved ${transformed.length} customers to ${filename}`);
}

// ============================================
// DATABASE INSERT - PostgreSQL (batched, safe)
// ============================================

const { Pool } = require('pg');
const pool = new Pool({
  ...CONFIG.database,
  max: 3,
  idleTimeoutMillis: 10000,
  connectionTimeoutMillis: 5000
});

/**
 * Build a multi-row INSERT ... ON CONFLICT SQL for a batch of transformed customers.
 * Returns { text, values }
 */
function buildUpsertQueryBatch(rows) {
  // Columns we persist (must match transformCustomer keys)
  const columns = [
    'customer_id', 'first_name', 'last_name', 'email', 'address_company',
    'address_1', 'address_2', 'city', 'state_code', 'country_code', 'zip_code',
    'phone', 'note', 'tax_exempt', 'created_at', 'updated_at', 'deleted_at',
    'verified_email', 'valid_email_address', 'email_marketing_state',
    'email_marketing_opt_in_level', 'email_consent_updated_at',
    'sms_marketing_state', 'sms_marketing_opt_in_level', 'sms_consent_updated_at',
    'display_name'
  ];

  const values = [];
  const valuePlaceholders = rows.map((r, rowIndex) => {
    const placeholders = columns.map((_, colIndex) => {
      const placeholderIndex = rowIndex * columns.length + colIndex + 1;
      return `$${placeholderIndex}`;
    });
    // push values in same order as columns
    values.push(
      r.customer_id, r.first_name, r.last_name, r.email, r.address_company,
      r.address_1, r.address_2, r.city, r.state_code, r.country_code, r.zip_code,
      r.phone, r.note, r.tax_exempt, r.created_at, r.updated_at, r.deleted_at,
      r.verified_email, r.valid_email_address, r.email_marketing_state,
      r.email_marketing_opt_in_level, r.email_consent_updated_at,
      r.sms_marketing_state, r.sms_marketing_opt_in_level, r.sms_consent_updated_at,
      r.display_name
    );
    return `(${placeholders.join(',')})`;
  });

  const insertSection = `INSERT INTO customers.raw_customers_shopify (${columns.join(', ')}) VALUES ${valuePlaceholders.join(', ')}`;

  // Build ON CONFLICT DO UPDATE - update all fields except customer_id
  const updateSets = columns
    .filter(c => c !== 'customer_id')
    .map(c => `${c} = EXCLUDED.${c}`)
    .join(', ');

  const text = `${insertSection}
    ON CONFLICT (customer_id) DO UPDATE SET ${updateSets}`;

  return { text, values };
}

async function insertBatch(transformedRows) {
  // If there are no rows, nothing to do
  if (!transformedRows || transformedRows.length === 0) return { success: 0, failed: 0 };

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const { text, values } = buildUpsertQueryBatch(transformedRows);
    await client.query(text, values);
    await client.query('COMMIT');
    return { success: transformedRows.length, failed: 0 };
  } catch (err) {
    await client.query('ROLLBACK').catch(() => { /* ignore rollback error */ });
    throw err;
  } finally {
    client.release();
  }
}

// ============================================
// BATCH PROCESSING (with retries & cooldown)
// ============================================

async function processBatch(customersBatch, batchNumber, totalBatches, outputFormat = 'database') {
  console.log(`\nProcessing batch ${batchNumber}/${totalBatches} (${customersBatch.length} customers)`);

  // Transform data
  const transformed = customersBatch.map(transformCustomer);

  if (outputFormat === 'database') {
    // Retry logic per batch
    let attempt = 0;
    while (attempt < CONFIG.batchRetries) {
      try {
        const result = await insertBatch(transformed);
        console.log(`✓ Batch ${batchNumber}: inserted ${result.success} rows`);
        break;
      } catch (err) {
        attempt++;
        console.error(`\nError inserting batch ${batchNumber} (attempt ${attempt}):`, err.message || err);
        if (attempt >= CONFIG.batchRetries) {
          console.error(`✖ Batch ${batchNumber} failed after ${attempt} attempts`);
          throw err;
        } else {
          const backoff = 250 * attempt;
          console.log(`→ Retrying batch ${batchNumber} after ${backoff}ms...`);
          await sleep(backoff);
        }
      }
    }

    // COOL DOWN between batches to ease DB pressure
    await sleep(CONFIG.batchCooldownMs);

  } else if (outputFormat === 'csv') {
    const filename = `customers_batch_${batchNumber}.csv`;
    saveAsCSV(customersBatch, filename);
  } else if (outputFormat === 'json') {
    const filename = `customers_batch_${batchNumber}.json`;
    saveAsJSON(customersBatch, filename);
  }

  console.log(`✓ Batch ${batchNumber} complete`);
}

// ============================================
// MAIN EXECUTION
// ============================================

async function main() {
  try {
    console.log('🚀 Starting Shopify Customer Import\n');
    console.log('DEBUG path:', CONFIG.localJsonlFile);

    const customers = await parseJsonlFile(CONFIG.localJsonlFile);
    console.log(`\n📊 Total customers to process: ${customers.length}\n`);

    if (customers.length === 0) {
      console.log('No customers found — exiting.');
      return;
    }

    // Show sample
    if (customers.length > 0) {
      console.log('Sample customer data:');
      console.log('- ID:', customers[0].id);
      console.log('- Email:', customers[0].email);
      console.log('- Name:', customers[0].displayName);
      console.log('- Orders:', customers[0].orders?.length || 0);
      console.log('- Addresses:', customers[0].addresses?.length || 0);
      console.log();
    }

    // Choose output format: 'database', 'csv', 'json'
    const OUTPUT_FORMAT = 'database';

    if (OUTPUT_FORMAT === 'csv' || OUTPUT_FORMAT === 'json') {
      if (OUTPUT_FORMAT === 'csv') saveAsCSV(customers);
      else saveAsJSON(customers);
      return;
    }

    // Process in batches
    const batches = [];
    for (let i = 0; i < customers.length; i += CONFIG.batchSize) {
      batches.push(customers.slice(i, i + CONFIG.batchSize));
    }

    console.log(`Processing in ${batches.length} batches of ${CONFIG.batchSize}\n`);

    for (let i = 0; i < batches.length; i++) {
      await processBatch(batches[i], i + 1, batches.length, OUTPUT_FORMAT);
    }

    console.log('\n✅ All customers processed successfully!');
  } catch (err) {
    console.error('\n❌ Fatal Error:', err);
    process.exit(1);
  } finally {
    // graceful pool shutdown
    try {
      await pool.end();
    } catch (e) {
      // ignore
    }
  }
}

main();
