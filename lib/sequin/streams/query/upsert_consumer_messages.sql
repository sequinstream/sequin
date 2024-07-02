WITH input_data(consumer_id, message_subject, message_seq) AS (
  SELECT * FROM UNNEST(
    :consumer_ids::uuid[],
    :message_subjects::text[],
    :message_seqs::bigint[]
  )
)
INSERT INTO streams.consumer_messages
    (consumer_id, message_subject, message_seq, state, updated_at, inserted_at)
SELECT 
  consumer_id,
  message_subject,
  message_seq,
  'available'::streams.consumer_message_state AS state,
  NOW() AS updated_at,
  NOW() AS inserted_at
FROM input_data
ON CONFLICT (consumer_id, message_subject)
DO UPDATE SET
  state = CASE streams.consumer_messages.state
    WHEN 'acked'::streams.consumer_message_state THEN 'available'::streams.consumer_message_state
    WHEN 'available'::streams.consumer_message_state THEN 'available'::streams.consumer_message_state
    WHEN 'delivered'::streams.consumer_message_state THEN 'pending_redelivery'::streams.consumer_message_state
    WHEN 'pending_redelivery'::streams.consumer_message_state THEN 'pending_redelivery'::streams.consumer_message_state
  END,
  message_seq = EXCLUDED.message_seq,
  updated_at = NOW()
WHERE EXCLUDED.message_seq > streams.consumer_messages.message_seq
