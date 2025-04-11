/**
 * This script generates SQL to:
 * 1. Create the wallet_transactions table
 * 2. Insert 72,000 rows of data in 1,000-row batches
 * 3. Generate UPDATE statements for each 1,000-row batch
 *
 * The format matches the example provided.
 */

// Save this file as generate-wallet-transactions.js
// Run with: node generate-wallet-transactions.js > wallet_transactions.sql

// Configuration
const TOTAL_ROWS = 72000;
const BATCH_SIZE = 1000;
const START_ID = 1; // If you want to start IDs from a different number

// Helper functions
function generateUUID() {
  return 'xxxxxxxx-xxxx-5xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
    const r = Math.random() * 16 | 0;
    const v = c === 'x' ? r : (r & 0x3 | 0x8);
    return v.toString(16);
  });
}

function generateTimestamp() {
  // Generate a timestamp in 2023 similar to the example
  const date = new Date(2023, 7, 31, 13, 49, 53); // Base date: 2023-08-31T13:49:53
  const randomOffset = Math.floor(Math.random() * 8640000); // Random offset within 100 days (in seconds)
  date.setSeconds(date.getSeconds() + randomOffset);
  return date.toISOString().replace(/\.\d+Z$/, "");
}

// Generate the CREATE TABLE statement
console.log(`-- Create the wallet_transactions table
CREATE TABLE IF NOT EXISTS wallet_transactions (
    id INT PRIMARY KEY,
    user_uuid UUID NOT NULL,
    amount VARCHAR(20) NOT NULL,
    old_balance DECIMAL(15, 2) NOT NULL,
    new_balance DECIMAL(15, 2) NOT NULL,
    transaction_type VARCHAR(50) NOT NULL,
    payment_uuid UUID,
    batch_id UUID,
    credit_type VARCHAR(50),
    date TIMESTAMP NOT NULL
);
`);

// Generate INSERT statements in batches
for (let batchStart = START_ID; batchStart < START_ID + TOTAL_ROWS; batchStart += BATCH_SIZE) {
  const batchEnd = Math.min(batchStart + BATCH_SIZE - 1, START_ID + TOTAL_ROWS - 1);
  
  console.log(`-- Batch ${batchStart}-${batchEnd} (${BATCH_SIZE} rows)`);
  console.log(`INSERT INTO wallet_transactions (id, user_uuid, amount, old_balance, new_balance, transaction_type, payment_uuid, batch_id, credit_type, date)`);
  console.log(`VALUES`);
  
  const commonBatchId = "7be3dfde-df28-4246-945f-25b70c3b3244";
  const creditTypes = ['withdrawable', 'deposit'];
  
  for (let id = batchStart; id <= batchEnd; id++) {
    const amountValue = "$" + (Math.random() * 100).toFixed(2);
    const oldBalance = 0;
    const amountNumber = parseFloat(amountValue.replace('$', ''));
    let newBalance, transactionType;
    
    if (id % 2 === 0) {
      transactionType = "add";
      newBalance = amountNumber;
    } else {
      transactionType = "remove";
      newBalance = -amountNumber;
    }
    
    const values = [
      id,
      `'${generateUUID()}'`,
      `'${amountValue}'`,
      oldBalance,
      newBalance,
      `'${transactionType}'`,
      `'${generateUUID()}'`,
      `'${commonBatchId}'`,
      `'${creditTypes[id % creditTypes.length]}'`,
      `'${generateTimestamp()}'`
    ];
    
    console.log(`(${values.join(", ")})${id < batchEnd ? ',' : ''}`);
  }
  
  console.log(`ON CONFLICT (id) DO UPDATE SET 
    old_balance = excluded.old_balance, 
    new_balance = excluded.new_balance;
`);
}

// Generate UPDATE statements for each batch
console.log(`-- Batch UPDATE statements`);
for (let batchStart = START_ID; batchStart < START_ID + TOTAL_ROWS; batchStart += BATCH_SIZE) {
  const batchEnd = Math.min(batchStart + BATCH_SIZE - 1, START_ID + TOTAL_ROWS - 1);
  
  console.log(`-- Update Batch ${batchStart}-${batchEnd}`);
  console.log(`UPDATE wallet_transactions 
SET 
    old_balance = new_balance - 10, 
    new_balance = new_balance + 5
WHERE id BETWEEN ${batchStart} AND ${batchEnd};
`);
}

// Add a convenience function to clear all the data if needed
console.log(`-- Convenience function to clear all data if needed
-- TRUNCATE TABLE wallet_transactions;
`);
