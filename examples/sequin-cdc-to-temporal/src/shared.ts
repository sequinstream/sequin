/**
 * Shared configuration constants used across the application.
 * This helps maintain consistency and makes it easy to update values in one place.
 */

/**
 * The name of the task queue used by both the worker and client.
 * This must match between the worker and any code that starts workflows.
 */
export const TASK_QUEUE_NAME = "user-deletion-workflow-queue";