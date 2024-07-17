# To list streams

sequin stream ls

# To show stream info

sequin stream info [stream-id]

# To add a new stream

sequin stream add [slug]

# To remove a stream

sequin stream rm [stream-id]

# To send a message to a stream

sequin stream send [stream-id] [key] [data]

# To view messages in a stream

sequin stream view [stream-id] [--filter=<pattern>] [--last=<N>] [--first=<N>] [--table]
