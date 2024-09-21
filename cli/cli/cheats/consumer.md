# To list consumers

sequin consumer ls

# To add a new consumer

sequin consumer add [name]

# To show consumer information

sequin consumer info [consumer]

# To receive messages for a consumer (default batch size is 1, no automatic ack)

sequin consumer receive [consumer]

# To receive messages for a consumer and acknowledge them

sequin consumer receive [consumer] --ack

# To receive a batch of messages for a consumer

sequin consumer receive [consumer] --batch-size 10

# To peek at messages for a consumer (default shows last 10 messages)

sequin consumer peek [consumer]

# To peek only at pending messages for a consumer

sequin consumer peek [consumer] --pending

# To peek at the last N messages for a consumer

sequin consumer peek [consumer] --last 20

# To peek at the first N messages for a consumer

sequin consumer peek [consumer] --first 20

# To ack a message

sequin consumer ack [consumer] [ack-id]

# To nack a message

sequin consumer nack [consumer] [ack-id]

# To edit a consumer

sequin consumer edit [consumer]
