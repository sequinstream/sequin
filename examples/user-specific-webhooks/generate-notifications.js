const { Pool } = require('pg');
const { setTimeout } = require('timers/promises');

// Postgres connection (to our 'sequin' DB on localhost:5433)
const dbPool = new Pool({
  connectionString: "postgres://postgres:postgres@localhost:5432/postgres",
});

async function getExistingUserIds() {
  const result = await dbPool.query('SELECT id FROM users');
  return result.rows.map(row => row.id);
}

async function generateNotifications() {
  let count = 0;
  const startTime = Date.now();

  // Get list of existing user IDs
  const userIds = await getExistingUserIds();
  if (userIds.length === 0) {
    console.error('No users found in the database. Please create some users first.');
    process.exit(1);
  }

  console.log(`Found ${userIds.length} users. Starting notification generation...`);

  while (true) {
    try {
      // Insert 100 notifications at once
      const values = Array.from({ length: 100 }, (_, i) => {
        const userId = userIds[Math.floor(Math.random() * userIds.length)]; // Random user from existing users
        const message = `Notification ${count + i + 1} for user ${userId}`;
        return [userId, message];
      });

      const query = `
        INSERT INTO notifications (user_id, message)
        VALUES ${values.map((_, i) => `($${i * 2 + 1}, $${i * 2 + 2})`).join(',')}
      `;

      await dbPool.query(query, values.flat());
      count += 100;

      // Log progress every 1000 notifications
      if (count % 1000 === 0) {
        const elapsedSeconds = (Date.now() - startTime) / 1000;
        const rate = count / elapsedSeconds;
        console.log(`Generated ${count} notifications at ${rate.toFixed(2)} notifications/second`);
      }

      // Wait for 1 second before next batch
      await setTimeout(1000);
    } catch (error) {
      console.error('Error generating notifications:', error);
      // Wait a bit before retrying
      await setTimeout(5000);
    }
  }
}

// Start generating notifications
generateNotifications().catch(console.error);