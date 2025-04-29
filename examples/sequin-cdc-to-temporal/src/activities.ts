/**
 * This file contains the activities (individual tasks) that make up our workflow.
 * Activities are the building blocks of Temporal workflows and represent the actual work being done.
 */

/**
 * Cleans up a user's data in external systems when their account is deleted.
 * This is a simulation - in a real application, this would make API calls to:
 * - Revoke access tokens
 * - Remove from mailing lists
 * - Delete data from third-party services
 * - etc.
 */
export async function cleanUpExternalSystems(userId: string): Promise<void> {
  console.log(`[*] Performing external cleanup for user ${userId}...`);
  // Simulated delay to represent real API calls
  await new Promise(resolve => setTimeout(resolve, 1000));
}

/**
 * Sends a confirmation message to the user after their account deletion is complete.
 * In a real application, this would:
 * - Send an email notification
 * - Or send an SMS
 * - Or update a notification system
 */
export async function sendDeletionConfirmation(email: string): Promise<void> {
  console.log(`[*] Sending account deletion confirmation to ${email}...`);
  // Simulated delay to represent sending a real notification
  await new Promise(resolve => setTimeout(resolve, 500));
}
