// Shopify Bulk Operation JSONL Parser
// Handles the actual Shopify JSONL format with embedded orders

const fs = require('fs');
const readline = require('readline');
const https = require('https');

// ============================================
// CONFIGURATION
// ============================================

const CONFIG = {
  
  // Local file path (if you've already downloaded it)
  localJsonlFile: './Downloads/HTTP_Request.json',

  // Use local file or download from URL?
  useLocalFile: true, // Set to true since you already have the file
  
  // Batch size for database inserts
  batchSize: 100,
  
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
// DOWNLOAD JSONL FILE
// ============================================

async function downloadFile(url, outputPath) {
  return new Promise((resolve, reject) => {
    console.log('Downloading JSONL file...');
    const file = fs.createWriteStream(outputPath);
    
    https.get(url, (response) => {
      const totalSize = parseInt(response.headers['content-length'], 10);
      let downloaded = 0;
      
      response.on('data', (chunk) => {
        downloaded += chunk.length;
        const percent = ((downloaded / totalSize) * 100).toFixed(2);
        process.stdout.write(`\rDownloading: ${percent}%`);
      });
      
      response.pipe(file);
      
      file.on('finish', () => {
        file.close();
        console.log('\n✓ Download complete!');
        resolve(outputPath);
      });
    }).on('error', (err) => {
      fs.unlink(outputPath, () => {});
      reject(err);
    });
  });
}

// ============================================
// STREAM PARSE JSONL
// ============================================

async function parseJsonlStream(filePath) {
  return new Promise((resolve, reject) => {
    const customers = {};
    let lineCount = 0;
    let orderCount = 0;
    
    const fileStream = fs.createReadStream(filePath);
    const rl = readline.createInterface({
      input: fileStream,
      crlfDelay: Infinity
    });
    
    console.log('Parsing JSONL file...');
    
    rl.on('line', (line) => {
      if (line.trim() === '') return;
      
      lineCount++;
      if (lineCount % 1000 === 0) {
        process.stdout.write(`\rProcessed ${lineCount} lines...`);
      }
      
      try {
        const obj = JSON.parse(line);
        
        // Check if this is a Customer object
        if (obj.id && obj.id.includes('/Customer/')) {
          // Initialize orders array if not present
          if (!obj.orders) {
            obj.orders = [];
          }
          customers[obj.id] = obj;
        }
        // Check if this is an Order object with a parent reference
        else if (obj.id && obj.id.includes('/Order/') && obj.__parentId) {
          orderCount++;
          // Add this order to its parent customer
          if (customers[obj.__parentId]) {
            customers[obj.__parentId].orders.push(obj);
          }
        }
      } catch (err) {
        console.error(`\nError parsing line ${lineCount}:`, err.message);
        console.error('Line content:', line.substring(0, 200));
      }
    });
    
    rl.on('close', () => {
      console.log(`\n✓ Parsed ${lineCount} lines`);
      console.log(`✓ Found ${Object.keys(customers).length} customers`);
      console.log(`✓ Found ${orderCount} orders`);
      resolve(Object.values(customers));
    });
    
    rl.on('error', reject);
  });
}

// ============================================
// TRANSFORM CUSTOMER DATA FOR DATABASE
// ============================================

function transformCustomer(customer) {
  // Extract Shopify ID number from GID (last numbers after final /)
  const extractId = (gid) => {
    if (!gid) return null;
    const match = gid.match(/\/(\d+)(\?|$)/);
    return match ? match[1] : null;
  };
  
  return {
    // Primary fields - matching your exact schema
    customer_id: extractId(customer.id),
    first_name: customer.firstName,
    last_name: customer.lastName,
    email: customer.email,
    display_name: customer.displayName,
    phone: customer.phone || customer.defaultAddress?.phone,
    
    // Default address fields
    address_company: customer.defaultAddress?.company,
    address_1: customer.defaultAddress?.address1,
    address_2: customer.defaultAddress?.address2,
    city: customer.defaultAddress?.city,
    state_code: customer.defaultAddress?.provinceCode,
    country_code: customer.defaultAddress?.countryCode,
    zip_code: customer.defaultAddress?.zip,
    
    // Additional info
    note: customer.note,
    tax_exempt: customer.taxExempt,
    verified_email: customer.verifiedEmail,
    valid_email_address: customer.validEmailAddress,
    
    // Dates
    created_at: customer.createdAt,
    updated_at: customer.updatedAt,
    deleted_at: null, // Not provided in bulk operation
    
    // Marketing consent
    email_marketing_state: customer.emailMarketingConsent?.marketingState,
    email_marketing_opt_in_level: customer.emailMarketingConsent?.marketingOptInLevel,
    email_consent_updated_at: customer.emailMarketingConsent?.consentUpdatedAt,
    
    sms_marketing_state: customer.smsMarketingConsent?.marketingState,
    sms_marketing_opt_in_level: customer.smsMarketingConsent?.marketingOptInLevel,
    sms_consent_updated_at: customer.smsMarketingConsent?.consentUpdatedAt
  };
}

// ============================================
// SAVE AS CSV (Simple Option)
// ============================================

async function saveAsCSV(customers, filename = 'customers_export.csv') {
  const transformed = customers.map(transformCustomer);
  
  if (transformed.length === 0) {
    console.log('No customers to export');
    return;
  }
  
  // Get headers from first customer
  const headers = Object.keys(transformed[0]);
  
  // Create CSV content
  let csv = headers.join(',') + '\n';
  
  transformed.forEach(customer => {
    const row = headers.map(header => {
      let value = customer[header];
      
      // Handle null/undefined
      if (value === null || value === undefined) {
        return '';
      }
      
      // Convert to string and escape quotes
      value = String(value).replace(/"/g, '""');
      
      // Wrap in quotes if contains comma, newline, or quote
      if (value.includes(',') || value.includes('\n') || value.includes('"')) {
        value = `"${value}"`;
      }
      
      return value;
    });
    
    csv += row.join(',') + '\n';
  });
  
  fs.writeFileSync(filename, csv);
  console.log(`✓ Saved ${transformed.length} customers to ${filename}`);
}

// ============================================
// SAVE AS JSON (Simple Option)
// ============================================

async function saveAsJSON(customers, filename = 'customers_export.json') {
  const transformed = customers.map(transformCustomer);
  fs.writeFileSync(filename, JSON.stringify(transformed, null, 2));
  console.log(`✓ Saved ${transformed.length} customers to ${filename}`);
}

// ============================================
// DATABASE INSERT - PostgreSQL
// ============================================

async function insertToPostgreSQL(customers) {
  const { Pool } = require('pg');
  const pool = new Pool(CONFIG.database);
  
  try {
    await pool.query('BEGIN');
    
    const insertQuery = `
      INSERT INTO customers.raw_customers_shopify (
        customer_id, first_name, last_name, email, address_company,
        address_1, address_2, city, state_code, country_code, zip_code,
        phone, note, tax_exempt, created_at, updated_at, deleted_at,
        verified_email, valid_email_address, email_marketing_state,
        email_marketing_opt_in_level, email_consent_updated_at,
        sms_marketing_state, sms_marketing_opt_in_level, sms_consent_updated_at,
        display_name
      ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
        $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26
      )
      ON CONFLICT (customer_id) 
      DO UPDATE SET
        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name,
        email = EXCLUDED.email,
        address_company = EXCLUDED.address_company,
        address_1 = EXCLUDED.address_1,
        address_2 = EXCLUDED.address_2,
        city = EXCLUDED.city,
        state_code = EXCLUDED.state_code,
        country_code = EXCLUDED.country_code,
        zip_code = EXCLUDED.zip_code,
        phone = EXCLUDED.phone,
        note = EXCLUDED.note,
        tax_exempt = EXCLUDED.tax_exempt,
        updated_at = EXCLUDED.updated_at,
        verified_email = EXCLUDED.verified_email,
        valid_email_address = EXCLUDED.valid_email_address,
        email_marketing_state = EXCLUDED.email_marketing_state,
        email_marketing_opt_in_level = EXCLUDED.email_marketing_opt_in_level,
        email_consent_updated_at = EXCLUDED.email_consent_updated_at,
        sms_marketing_state = EXCLUDED.sms_marketing_state,
        sms_marketing_opt_in_level = EXCLUDED.sms_marketing_opt_in_level,
        sms_consent_updated_at = EXCLUDED.sms_consent_updated_at,
        display_name = EXCLUDED.display_name
    `;
    
    let successCount = 0;
    let errorCount = 0;
    
    for (const customer of customers) {
      try {
        const values = [
          customer.customer_id,
          customer.first_name,
          customer.last_name,
          customer.email,
          customer.address_company,
          customer.address_1,
          customer.address_2,
          customer.city,
          customer.state_code,
          customer.country_code,
          customer.zip_code,
          customer.phone,
          customer.note,
          customer.tax_exempt,
          customer.created_at,
          customer.updated_at,
          customer.deleted_at,
          customer.verified_email,
          customer.valid_email_address,
          customer.email_marketing_state,
          customer.email_marketing_opt_in_level,
          customer.email_consent_updated_at,
          customer.sms_marketing_state,
          customer.sms_marketing_opt_in_level,
          customer.sms_consent_updated_at,
          customer.display_name
        ];
        
        await pool.query(insertQuery, values);
        successCount++;
      } catch (err) {
        errorCount++;
        console.error(`\nError inserting customer ${customer.customer_id}:`, err.message);
      }
    }
    
    await pool.query('COMMIT');
    console.log(`✓ Successfully inserted/updated ${successCount} customers`);
    if (errorCount > 0) {
      console.log(`⚠ Failed to insert ${errorCount} customers`);
    }
    
  } catch (err) {
    await pool.query('ROLLBACK');
    console.error('Database transaction error:', err);
    throw err;
  } finally {
    await pool.end();
  }
}

// ============================================
// BATCH PROCESSING
// ============================================

async function processBatch(customers, batchNumber, totalBatches, outputFormat = 'csv') {
  console.log(`\nProcessing batch ${batchNumber}/${totalBatches} (${customers.length} customers)`);
  
  // Transform data
  const transformed = customers.map(transformCustomer);
  
  // Choose output method
  if (outputFormat === 'database') {
    await insertToPostgreSQL(transformed);
  } else if (outputFormat === 'csv') {
    const filename = `customers_batch_${batchNumber}.csv`;
    await saveAsCSV(customers, filename);
  } else if (outputFormat === 'json') {
    const filename = `customers_batch_${batchNumber}.json`;
    await saveAsJSON(customers, filename);
  }
  
  console.log(`✓ Batch ${batchNumber} complete`);
}

// ============================================
// MAIN EXECUTION
// ============================================

async function main() {
  try {
    console.log('🚀 Starting Shopify Customer Import\n');
    
    // Step 1: Get the JSONL file
    let filePath;
    if (CONFIG.useLocalFile) {
      filePath = CONFIG.localJsonlFile;
      console.log(`Using local file: ${filePath}`);
    } else {
      filePath = './downloaded_customers.jsonl';
      await downloadFile(CONFIG.bulkOperationUrl, filePath);
    }
    
    // Step 2: Parse JSONL
    const customers = await parseJsonlStream(filePath);
    console.log(`\n📊 Total customers to process: ${customers.length}\n`);
    
    // Show sample customer
    if (customers.length > 0) {
      console.log('Sample customer data:');
      console.log('- Email:', customers[0].email);
      console.log('- Name:', customers[0].displayName);
      console.log('- Orders:', customers[0].orders?.length || 0);
      console.log('- Addresses:', customers[0].addresses?.length || 0);
      console.log();
    }
    
    // Step 3: Choose output format
    // Options: 'database', 'csv', 'json'
    const OUTPUT_FORMAT = 'database'; // Change this to your preferred format
    
    if (OUTPUT_FORMAT === 'csv' || OUTPUT_FORMAT === 'json') {
      // Save all customers to single file
      if (OUTPUT_FORMAT === 'csv') {
        await saveAsCSV(customers);
      } else {
        await saveAsJSON(customers);
      }
    } else {
      // Process in batches for database
      const batches = [];
      for (let i = 0; i < customers.length; i += CONFIG.batchSize) {
        batches.push(customers.slice(i, i + CONFIG.batchSize));
      }
      
      console.log(`Processing in ${batches.length} batches of ${CONFIG.batchSize}\n`);
      
      for (let i = 0; i < batches.length; i++) {
        await processBatch(batches[i], i + 1, batches.length, OUTPUT_FORMAT);
      }
    }
    
    console.log('\n✅ All customers processed successfully!');
    
  } catch (err) {
    console.error('\n❌ Error:', err);
    process.exit(1);
  }
}

// Run the script
main();