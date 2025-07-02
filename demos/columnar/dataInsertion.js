const dotenv = require('dotenv');
const columnarClient = require('./columnarClient');

// Load environment variables
dotenv.config();

// Configuration
const TOTAL_DOCUMENTS = 50000;
const BATCH_SIZE = 1000; // Insert documents in batches for better performance
const TARGET_DATABASE = 'test-data';
const TARGET_SCOPE = 'generated';
const TARGET_COLLECTION = 'sample_docs';

// Generate random data for document attributes
function generateRandomData() {
  const firstNames = ['John', 'Jane', 'Mike', 'Sarah', 'David', 'Lisa', 'Chris', 'Emma', 'Alex', 'Maria'];
  const lastNames = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez'];
  const cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose'];
  const companies = ['TechCorp', 'DataSys', 'CloudTech', 'InnovateCo', 'GlobalTech', 'SmartSolutions', 'DigitalWorks', 'FutureTech', 'NextGen', 'BrightIdeas'];
  const departments = ['Engineering', 'Marketing', 'Sales', 'HR', 'Finance', 'Operations', 'Support', 'Research', 'Product', 'Quality'];
  
  return {
    firstName: firstNames[Math.floor(Math.random() * firstNames.length)],
    lastName: lastNames[Math.floor(Math.random() * lastNames.length)],
    city: cities[Math.floor(Math.random() * cities.length)],
    company: companies[Math.floor(Math.random() * companies.length)],
    department: departments[Math.floor(Math.random() * departments.length)]
  };
}

// Generate a single document with 10 attributes
function generateDocument(id) {
  const randomData = generateRandomData();
  
  return {
    id: id, // Use numeric ID instead of string to avoid bigint conversion issues
    doc_key: `doc_${id}`, // Keep string version as separate field
    timestamp: new Date().toISOString(),
    name: `${randomData.firstName} ${randomData.lastName}`,
    age: Math.floor(Math.random() * 50) + 20, // Age between 20-70
    email: `${randomData.firstName.toLowerCase()}.${randomData.lastName.toLowerCase()}@${randomData.company.toLowerCase()}.com`,
    city: randomData.city,
    company: randomData.company,
    department: randomData.department,
    salary: Math.floor(Math.random() * 100000) + 40000, // Salary between 40k-140k
    score: Math.round((Math.random() * 100) * 100) / 100, // Score between 0-100 with 2 decimal places
    active: Math.random() > 0.3 // 70% chance of being active
  };
}

// Generate a batch of documents
function generateBatch(startId, batchSize) {
  const batch = [];
  for (let i = 0; i < batchSize; i++) {
    batch.push(generateDocument(startId + i));
  }
  return batch;
}

// Insert a batch of documents using Columnar INSERT syntax
async function insertBatch(batch, batchNumber) {
  console.log(`Inserting batch ${batchNumber} (${batch.length} documents)...`);
  
  try {
    // Create individual INSERT statements for each document in the batch
    const insertPromises = batch.map(async (doc, index) => {
      const query = `
        INSERT INTO \`${TARGET_DATABASE}\`.\`${TARGET_SCOPE}\`.\`${TARGET_COLLECTION}\` ([{
          "id": ${doc.id},
          "doc_key": "${doc.doc_key}",
          "timestamp": "${doc.timestamp}",
          "name": "${doc.name}",
          "age": ${doc.age},
          "email": "${doc.email}",
          "city": "${doc.city}",
          "company": "${doc.company}",
          "department": "${doc.department}",
          "salary": ${doc.salary},
          "score": ${doc.score},
          "active": ${doc.active}
        }])
      `;
      
      try {
        return await columnarClient.submitRequest(query);
      } catch (error) {
        console.error(`Error inserting document ${doc.id}:`, error.message);
        throw error;
      }
    });
    
    // Wait for all inserts in this batch to complete
    await Promise.all(insertPromises);
    console.log(`✅ Batch ${batchNumber} completed successfully`);
    
  } catch (error) {
    console.error(`❌ Batch ${batchNumber} failed:`, error.message);
    throw error;
  }
}

// Alternative approach using a single bulk insert query with Columnar syntax
async function insertBatchBulk(batch, batchNumber) {
  console.log(`Inserting batch ${batchNumber} (${batch.length} documents) using bulk insert...`);
  
  try {
    // Create a single query with multiple JSON objects in array format
    const jsonObjects = batch.map(doc => `{
      "id": ${doc.id},
      "doc_key": "${doc.doc_key}",
      "timestamp": "${doc.timestamp}",
      "name": "${doc.name}",
      "age": ${doc.age},
      "email": "${doc.email}",
      "city": "${doc.city}",
      "company": "${doc.company}",
      "department": "${doc.department}",
      "salary": ${doc.salary},
      "score": ${doc.score},
      "active": ${doc.active}
    }`).join(',');
    
    const query = `
      INSERT INTO \`${TARGET_DATABASE}\`.\`${TARGET_SCOPE}\`.\`${TARGET_COLLECTION}\` ([
        ${jsonObjects}
      ])
    `;
    
    await columnarClient.submitRequest(query);
    console.log(`✅ Batch ${batchNumber} completed successfully (bulk insert)`);
    
  } catch (error) {
    console.error(`❌ Batch ${batchNumber} failed (bulk insert):`, error.message);
    throw error;
  }
}

// Create the target collection if it doesn't exist
async function createCollectionIfNotExists() {
  console.log(`Checking if collection ${TARGET_DATABASE}.${TARGET_SCOPE}.${TARGET_COLLECTION} exists...`);
  
  try {
    // Only create the collection since database and scope should already exist
    // Note: Couchbase Columnar doesn't support IF NOT EXISTS syntax
    const query = `CREATE COLLECTION \`${TARGET_DATABASE}\`.\`${TARGET_SCOPE}\`.\`${TARGET_COLLECTION}\``;
    
    try {
      await columnarClient.submitRequest(query);
      console.log(`✅ Created collection: ${TARGET_DATABASE}.${TARGET_SCOPE}.${TARGET_COLLECTION}`);
    } catch (error) {
      // Check if it's a "already exists" error (which is expected)
      if (error.message.includes('already exists') || error.message.includes('Already exists')) {
        console.log(`✅ Collection ${TARGET_DATABASE}.${TARGET_SCOPE}.${TARGET_COLLECTION} already exists - continuing`);
      } else {
        console.log(`⚠️ Could not create collection: ${error.message}`);
        console.log('⚠️ Continuing anyway - will try to insert data');
      }
    }
    
    console.log(`Collection setup complete`);
    
  } catch (error) {
    console.error('Error setting up collection:', error.message);
    console.log('⚠️ Continuing anyway - collection might already exist');
  }
}

// Verify insertion by counting documents
async function verifyInsertion() {
  console.log('\nVerifying insertion...');
  
  try {
    const countQuery = `SELECT COUNT(*) as total FROM \`${TARGET_DATABASE}\`.\`${TARGET_SCOPE}\`.\`${TARGET_COLLECTION}\``;
    const result = await columnarClient.submitRequest(countQuery);
    
    if (result && result.results && result.results.length > 0) {
      const count = result.results[0].total;
      console.log(`✅ Verification complete: ${count} documents found in collection`);
      return count;
    } else {
      console.log('⚠️ Could not verify document count');
      return 0;
    }
  } catch (error) {
    console.error('Error verifying insertion:', error.message);
    
    // If count fails, try a simpler query to check if collection exists
    try {
      console.log('Trying alternative verification...');
      const simpleQuery = `SELECT * FROM \`${TARGET_DATABASE}\`.\`${TARGET_SCOPE}\`.\`${TARGET_COLLECTION}\` LIMIT 1`;
      const simpleResult = await columnarClient.submitRequest(simpleQuery);
      if (simpleResult && simpleResult.results) {
        console.log('✅ Collection exists and contains data');
        return -1; // Unknown count but collection has data
      }
    } catch (simpleError) {
      console.error('Collection verification failed:', simpleError.message);
    }
    
    return 0;
  }
}

// Show sample of inserted data
async function showSampleData() {
  console.log('\nFetching sample data...');
  
  try {
    const sampleQuery = `SELECT * FROM \`${TARGET_DATABASE}\`.\`${TARGET_SCOPE}\`.\`${TARGET_COLLECTION}\` LIMIT 5`;
    const result = await columnarClient.submitRequest(sampleQuery);
    
    if (result && result.results && result.results.length > 0) {
      console.log('\nSample documents:');
      result.results.forEach((doc, index) => {
        console.log(`[${index + 1}] ${JSON.stringify(doc, null, 2)}`);
      });
    } else {
      console.log('No sample data found');
    }
  } catch (error) {
    console.error('Error fetching sample data:', error.message);
  }
}

// Main insertion function
async function insertDocuments() {
  console.log(`\n🚀 Starting data insertion process...`);
  console.log(`Target: ${TARGET_DATABASE}.${TARGET_SCOPE}.${TARGET_COLLECTION}`);
  console.log(`Documents to insert: ${TOTAL_DOCUMENTS}`);
  console.log(`Batch size: ${BATCH_SIZE}`);
  console.log(`Total batches: ${Math.ceil(TOTAL_DOCUMENTS / BATCH_SIZE)}`);
  
  const startTime = Date.now();
  let successfulInserts = 0;
  let failedInserts = 0;
  
  try {
    // Create collection if it doesn't exist
    await createCollectionIfNotExists();
    
    // Process in batches
    for (let i = 0; i < TOTAL_DOCUMENTS; i += BATCH_SIZE) {
      const batchNumber = Math.floor(i / BATCH_SIZE) + 1;
      const currentBatchSize = Math.min(BATCH_SIZE, TOTAL_DOCUMENTS - i);
      
      try {
        // Generate batch of documents
        const batch = generateBatch(i + 1, currentBatchSize);
        
        // Insert batch (try bulk insert first, fallback to individual inserts)
        try {
          await insertBatchBulk(batch, batchNumber);
          successfulInserts += currentBatchSize;
        } catch (bulkError) {
          console.log(`Bulk insert failed for batch ${batchNumber}, trying individual inserts...`);
          await insertBatch(batch, batchNumber);
          successfulInserts += currentBatchSize;
        }
        
        // Progress update
        const progress = Math.round((successfulInserts / TOTAL_DOCUMENTS) * 100);
        console.log(`Progress: ${progress}% (${successfulInserts}/${TOTAL_DOCUMENTS} documents)`);
        
      } catch (error) {
        console.error(`Batch ${batchNumber} failed completely:`, error.message);
        failedInserts += currentBatchSize;
      }
    }
    
    const endTime = Date.now();
    const duration = (endTime - startTime) / 1000;
    
    console.log(`\n📊 Data insertion completed!`);
    console.log(`Duration: ${duration.toFixed(2)} seconds`);
    console.log(`Successful inserts: ${successfulInserts}`);
    console.log(`Failed inserts: ${failedInserts}`);
    console.log(`Success rate: ${((successfulInserts / TOTAL_DOCUMENTS) * 100).toFixed(2)}%`);
    
    // Verify insertion
    const actualCount = await verifyInsertion();
    
    // Show sample data
    await showSampleData();
    
    return {
      success: successfulInserts,
      failed: failedInserts,
      duration: duration,
      actualCount: actualCount
    };
    
  } catch (error) {
    console.error('Critical error during data insertion:', error.message);
    throw error;
  }
}

// Run the insertion script
async function runInsertionScript() {
  console.log('🔄 Initializing Couchbase Columnar Data Insertion Script...');
  
  try {
    // Test basic connectivity
    const testQuery = 'SELECT 1 AS test';
    const testResult = await columnarClient.submitRequest(testQuery);
    
    if (testResult && testResult.results) {
      console.log('✅ Connection to Couchbase Columnar established');
    } else {
      throw new Error('Failed to establish connection to Couchbase Columnar');
    }
    
    // Run the insertion
    const results = await insertDocuments();
    
    console.log('\n🎉 Data insertion script completed successfully!');
    return results;
    
  } catch (error) {
    console.error('❌ Data insertion script failed:', error.message);
    process.exitCode = 1;
    throw error;
  }
}

// Export functions for testing
module.exports = {
  generateDocument,
  generateBatch,
  insertBatch,
  insertBatchBulk,
  createCollectionIfNotExists,
  verifyInsertion,
  showSampleData,
  insertDocuments,
  runInsertionScript
};

// Run the script if executed directly
if (require.main === module) {
  runInsertionScript()
    .then(results => {
      console.log('\n✅ Script execution completed:', results);
    })
    .catch(error => {
      console.error('\n❌ Script execution failed:', error.message);
      process.exit(1);
    });
} 