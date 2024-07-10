# Use the official Elixir image as a parent image
FROM elixir:1.17.1-otp-27

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Set MIX_ENV to prod for the build step
ENV MIX_ENV=prod

# Install any needed packages specified in mix.exs
RUN mix local.hex --force && \
    mix local.rebar --force && \
    mix deps.get

# Make port 4000 available to the world outside this container
EXPOSE 4000

# Run the app when the container launches
CMD ["mix", "run", "--no-halt"]