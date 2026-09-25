/**
 * Thin client for the {@code diavasi.data.v1} data plane.
 *
 * <p>The server owns the cursor. A caller joins a consumer group, receives
 * batches, and acks by {@code batch_id}. {@code record_id} can be 0, so this
 * package does not dedupe on it. Dropping the stream is how unacked batches
 * return. Reconnect with the same consumer id and the server replays them.
 */
package dev.diavasi.data;
