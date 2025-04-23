const express = require("express");
const axios = require("axios");
const { Pool } = require("pg");

const app = express();
app.use(express.json()); // Enable JSON body parsing

// Configuration – replace with your actual API token:
const SEQUIN_API_TOKEN = "YOUR_API_TOKEN_HERE"; // (e.g., the token you copied from the console)
const SEQUIN_API_BASE = "http://localhost:7376/api";
const PORT = 3333; // Port number for the Express server

// Postgres connection to your local database
const dbPool = new Pool({
    connectionString: "postgres://postgres:postgres@localhost:5432/postgres", // Replace with your database connection string
});

// Endpoint to subscribe a user to webhook notifications
app.post("/subscribe", async (req, res) => {
    const { userId, webhookUrl } = req.body;
    if (!userId || !webhookUrl) {
        return res.status(400).json({ error: "Missing userId or webhookUrl" });
    }
    try {
        // 1. Create a new HTTP endpoint in Sequin for the given webhook URL
        const endpointName = `user-${userId}-endpoint`;
        let endpointResp;
        try {
            endpointResp = await axios.post(
                `${SEQUIN_API_BASE}/destinations/http_endpoints`,
                {
                    name: endpointName,
                    url: webhookUrl,
                },
                {
                    headers: { Authorization: `Bearer ${SEQUIN_API_TOKEN}` },
                }
            );
        } catch (err) {
            if (err.response?.status === 401) {
                throw new Error('Invalid API token');
            } else if (err.response?.status === 400) {
                throw new Error(`Invalid request: ${err.response.data.message}`);
            } else {
                throw new Error(`Failed to create HTTP endpoint: ${err.message}`);
            }
        }

        if (!endpointResp?.data?.id) {
            throw new Error('Failed to get endpoint ID from response');
        }

        const endpointId = endpointResp.data.id;
        console.log(
            `Created HTTP endpoint (${endpointName}) with ID = ${endpointId}`
        );

        // 2. Create a new webhook sink for the notifications table, filtered to this user
        const sinkName = `user-${userId}-notifications`;
        let sinkResp;
        try {
            sinkResp = await axios.post(
                `${SEQUIN_API_BASE}/sinks`,
                {
                    name: sinkName,
                    status: "active",
                    database: "postgres", // source database name. Make sure this matches the name you provided in the Sequin console when you connected your database.
                    table: "public.notifications", // source table to watch. Note that Sequin requires the schema name in the table name.
                    filters: [
                        // filter to only stream this user's rows
                        {
                            column_name: "user_id",
                            operator: "=",
                            comparison_value: userId.toString(),
                        },
                    ],
                    destination: {
                        type: "webhook",
                        http_endpoint: endpointName, // reference the HTTP endpoint by name
                        http_endpoint_path: "",
                    },
                    // (By default, the sink will capture new inserts/updates/deletes in real-time)
                    actions: ["insert", "update", "delete"],
                },
                {
                    headers: { Authorization: `Bearer ${SEQUIN_API_TOKEN}` },
                }
            );
        } catch (err) {
            if (err.response?.status === 401) {
                throw new Error('Invalid API token');
            } else if (err.response?.status === 400) {
                throw new Error(`Invalid sink configuration: ${err.response.data.message}`);
            } else {
                throw new Error(`Failed to create sink: ${err.message}`);
            }
        }

        if (!sinkResp?.data?.id) {
            throw new Error('Failed to get sink ID from response');
        }

        const sinkId = sinkResp.data.id;
        console.log(`Created webhook sink (${sinkName}) with ID = ${sinkId}`);

        // 3. Save the subscription details in the database
        await dbPool.query(
            "INSERT INTO webhook_subscriptions(user_id, endpoint, sequin_sink_id) VALUES($1, $2, $3)",
            [userId, webhookUrl, sinkId]
        );

        res.json({ message: "Webhook subscription created", sinkId });
    } catch (err) {
        console.error(
            "Error creating subscription:",
            err.response?.data || err.message
        );
        return res
            .status(500)
            .json({ error: "Failed to create webhook subscription" });
    }
});

// Endpoint to start a backfill for a webhook sink
app.post("/backfill", async (req, res) => {
    const { sinkName } = req.body;
    if (!sinkName) {
        return res.status(400).json({ error: "Missing sinkName parameter" });
    }

    try {
        // Make a POST request to the Sequin Management API to start a backfill
        const backfillResp = await axios.post(
            `${SEQUIN_API_BASE}/sinks/${sinkName}/backfills`,
            {},
            {
                headers: { Authorization: `Bearer ${SEQUIN_API_TOKEN}` },
            }
        );

        res.json({
            message: "Backfill started successfully",
            backfillId: backfillResp.data.id,
            state: backfillResp.data.state,
        });
    } catch (err) {
        console.error(
            "Error starting backfill:",
            err.response?.data || err.message
        );
        return res.status(500).json({ error: "Failed to start backfill" });
    }
});

// Start the Express server
app.listen(PORT, () => {
    console.log(`Server listening on http://localhost:${PORT}`);
});
