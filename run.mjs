import fetch from "cross-fetch";
import { subscribeToQuery } from "datocms-listen";
import { EventSource } from 'eventsource';
import { setTimeout as asyncSetTimeout } from 'timers/promises';
// Configuration
const CONFIG = {
    maxConnections: 2500, // Maximum number of connections to establish
    connectionStep: 200, // How many connections to add in each batch
    stepIntervalMs: 1000, // Time between adding batches (milliseconds)
    monitorIntervalMs: 5000, // How often to log status (milliseconds)
    maxTestDurationMs: 600000, // Maximum test duration (10 minutes)
    token: "24c07f1e3c8e5344c4c915b388d8d2",
    baseUrl: 'https://graphql-listen-eu1.staging-datocms.com',
    query: `{ allPosts { id } }`
};
// Test statistics
const stats = {
    activeConnections: 0,
    totalConnectionAttempts: 0,
    successfulConnections: 0,
    failedConnections: 0,
    disconnections: 0,
    channelErrors: 0,
    genericErrors: 0,
    updates: 0,
    connectionStatuses: {},
    startTime: null,
    endTime: null
};
// Store all connection unsubscribe functions
const connections = [];
// Monitor active connection statuses
function updateConnectionStatus(id, status) {
    stats.connectionStatuses[id] = status;
}
// Create a single SSE connection
async function createConnection(id) {
    stats.totalConnectionAttempts++;
    try {
        let unsubscribe;
        const onUpdate = (update) => {
            stats.updates++;
            if (stats.updates % 500 === 0) {
                console.log(`Received ${stats.updates} updates`);
                console.log(update);
            }
        };
        const onStatusChange = (status) => {
            updateConnectionStatus(id, status);
            if (status === 'connected') {
                stats.successfulConnections++;
            }
            else if (status === 'closed') {
                stats.disconnections++;
                unsubscribe === null || unsubscribe === void 0 ? void 0 : unsubscribe();
            }
        };
        const onChannelError = (error) => {
            stats.channelErrors++;
            console.error(`[Connection ${id}] Error:`, error.message);
        };
        const onError = (error) => {
            stats.genericErrors++;
            console.error(`[Connection ${id}] Error:`, error.data);
        };
        unsubscribe = await subscribeToQuery({
            fetcher: fetch,
            eventSourceClass: EventSource,
            baseUrl: CONFIG.baseUrl,
            query: CONFIG.query,
            token: CONFIG.token,
            includeDrafts: true,
            onUpdate,
            onStatusChange,
            onChannelError,
            onError,
        });
        connections.push({ id, unsubscribe });
        stats.activeConnections++;
        return true;
    }
    catch (error) {
        stats.failedConnections++;
        console.error(`[Connection ${id}] Failed to establish:`, error instanceof Error ? error.message : String(error));
        return false;
    }
}
// Log current test statistics
function logStats() {
    if (!stats.startTime)
        return;
    const runningTime = Math.floor((Date.now() - stats.startTime) / 1000);
    // Count connections by status
    const statusCounts = {};
    Object.values(stats.connectionStatuses).forEach(status => {
        statusCounts[status] = (statusCounts[status] || 0) + 1;
    });
    console.log('\n------- LOAD TEST STATISTICS -------');
    console.log(`Test running for: ${runningTime} seconds`);
    console.log(`Active connections: ${stats.activeConnections}`);
    console.log(`Connection attempts: ${stats.totalConnectionAttempts}`);
    console.log(`Successful connections: ${stats.successfulConnections}`);
    console.log(`Failed connections: ${stats.failedConnections}`);
    console.log(`Disconnections: ${stats.disconnections}`);
    console.log(`Channel Errors: ${stats.channelErrors}`);
    console.log(`Generic Errors: ${stats.genericErrors}`);
    console.log(`Updates received: ${stats.updates}`);
    console.log('Connection statuses:', statusCounts);
    console.log('-----------------------------------\n');
}
// Close all connections and clean up
async function cleanup() {
    console.log(`\nShutting down ${connections.length} connections...`);
    const closePromises = connections.map(async (conn) => {
        try {
            await conn.unsubscribe();
            return true;
        }
        catch (err) {
            console.error(`Error closing connection ${conn.id}:`, err instanceof Error ? err.message : String(err));
            return false;
        }
    });
    await Promise.allSettled(closePromises);
    stats.endTime = Date.now();
    if (stats.startTime) {
        const testDurationSec = Math.floor((stats.endTime - stats.startTime) / 1000);
        console.log('\n------- FINAL TEST RESULTS -------');
        console.log(`Test duration: ${testDurationSec} seconds`);
        console.log(`Maximum concurrent connections: ${stats.successfulConnections}`);
        console.log(`Failed connections: ${stats.failedConnections}`);
    }
    process.exit(0);
}
// Run the load test
async function runLoadTest() {
    stats.startTime = Date.now();
    console.log(`Starting load test with max ${CONFIG.maxConnections} connections`);
    console.log(`Adding ${CONFIG.connectionStep} connections every ${CONFIG.stepIntervalMs / 1000} seconds`);
    // Set up monitoring
    const statsInterval = setInterval(logStats, CONFIG.monitorIntervalMs);
    // Set test timeout
    const timeout = setTimeout(() => {
        console.log('Maximum test duration reached');
        clearInterval(statsInterval);
        cleanup();
    }, CONFIG.maxTestDurationMs);
    try {
        // Add connections in batches
        for (let i = 0; i < CONFIG.maxConnections; i += CONFIG.connectionStep) {
            console.log(`Adding batch of ${CONFIG.connectionStep} connections (${i + 1} to ${Math.min(i + CONFIG.connectionStep, CONFIG.maxConnections)})`);
            const batchPromises = [];
            for (let j = 0; j < CONFIG.connectionStep && i + j < CONFIG.maxConnections; j++) {
                const connectionId = i + j + 1;
                batchPromises.push(createConnection(connectionId));
            }
            // Wait before adding the next batch
            await asyncSetTimeout(CONFIG.stepIntervalMs);
        }
        await asyncSetTimeout(1000000000);
    }
    catch (error) {
        console.error('Test failed with error:', error instanceof Error ? error.message : String(error));
    }
    finally {
        clearTimeout(timeout);
        clearInterval(statsInterval);
        await cleanup();
    }
}
// Handle process termination
process.on('SIGINT', async () => {
    console.log('\nReceived SIGINT, shutting down gracefully');
    await cleanup();
});
// Start the load test
runLoadTest();

