#!/bin/bash

# Database connection details
DB_HOST="localhost"
DB_PORT="5432"
DB_NAME="dune"
DB_USER="postgres"
DB_PASS="postgres"

# Function to execute SQL commands
execute_sql() {
    PGPASSWORD=$DB_PASS psql -h $DB_HOST -p $DB_PORT -d $DB_NAME -U $DB_USER -c "$1"
}

# Function to get a random character ID
get_random_character() {
    execute_sql "SELECT id FROM characters ORDER BY RANDOM() LIMIT 1;" | sed -n 3p | xargs
}

# Function to update kills
update_kills() {
    local id=$1
    local new_kills=$((RANDOM % 10))
    execute_sql "UPDATE characters SET kills = kills + $new_kills WHERE id = $id;"
    echo "Updated kills for character $id"
}

# Function to update house
update_house() {
    local id=$1
    local houses=("Atreides" "Harkonnen" "Corrino" "Fremen" "Bene Gesserit" "Spacing Guild")
    local new_house=${houses[$RANDOM % ${#houses[@]}]}
    execute_sql "UPDATE characters SET house = '$new_house' WHERE id = $id;"
    echo "Updated house for character $id to $new_house"
}

# Function to update name
update_name() {
    local id=$1
    local names=("Paul" "Leto" "Jessica" "Duncan" "Gurney" "Thufir" "Stilgar" "Chani" "Alia" "Feyd-Rautha")
    local new_name=${names[$RANDOM % ${#names[@]}]}
    execute_sql "UPDATE characters SET name = '$new_name' WHERE id = $id;"
    echo "Updated name for character $id to $new_name"
}

# Function to update dead status
update_dead() {
    local id=$1
    local new_status
    if [ $((RANDOM % 2)) -eq 0 ]; then
        new_status="false"
    else
        new_status="true"
    fi
    execute_sql "UPDATE characters SET dead = $new_status WHERE id = $id;"
    echo "Updated dead status for character $id to $new_status"
}

# Main loop
while true; do
    character_id=$(get_random_character)
    
    case $((RANDOM % 4)) in
        0) update_kills $character_id ;;
        1) update_house $character_id ;;
        2) update_name $character_id ;;
        3) update_dead $character_id ;;
    esac
    
    sleep 2
done
