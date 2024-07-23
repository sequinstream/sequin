WITH input_data(consumer_id, message_key, message_seq) AS (
  SELECT * FROM UNNEST(
    :consumer_ids::uuid[],
    :message_keys::text[],
    :message_seqs::bigint[]
  )
)
INSERT INTO sequin_streams.consumer_messages
    (consumer_id, message_key, message_seq, state, updated_at, inserted_at)
SELECT 
  consumer_id,
  message_key,
  message_seq,
  'available'::sequin_streams.consumer_message_state AS state,
  NOW() AS updated_at,
  NOW() AS inserted_at
FROM input_data
ON CONFLICT (consumer_id, message_key)
DO UPDATE SET
  state = CASE sequin_streams.consumer_messages.state
    WHEN 'acked'::sequin_streams.consumer_message_state THEN 'available'::sequin_streams.consumer_message_state
    WHEN 'available'::sequin_streams.consumer_message_state THEN 'available'::sequin_streams.consumer_message_state
    WHEN 'delivered'::sequin_streams.consumer_message_state THEN 'pending_redelivery'::sequin_streams.consumer_message_state
    WHEN 'pending_redelivery'::sequin_streams.consumer_message_state THEN 'pending_redelivery'::sequin_streams.consumer_message_state
  END,
  message_seq = EXCLUDED.message_seq,
  updated_at = NOW()
WHERE EXCLUDED.message_seq > sequin_streams.consumer_messages.message_seq
