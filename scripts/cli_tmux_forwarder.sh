#!/bin/bash

# Usage / Help function
show_usage() {
    echo "Usage:"
    echo "  Ctrl+R: Reload CLI in all other panes"
    echo "  s: Send Enter to all other panes"
    echo "  Any other key: Forward to all other panes"
}

# Function to send keystrokes to all other panes
send_to_other_panes() {
    local key="$1"
    tmux list-panes -F '#{pane_id}' | grep -v "$(tmux display-message -p '#{pane_id}')" | xargs -I {} tmux send-keys -t {} "$key"
}

# Function to reload CLI in all other panes
reload_cli() {
    tmux list-panes -F '#{pane_id}' | grep -v "$(tmux display-message -p '#{pane_id}')" | xargs -I {} tmux send-keys -t {} C-c C-c "clear && make cli observe" Enter
}

# Show usage information
show_usage

# Main loop to capture and forward keystrokes
while IFS= read -r -n1 -s key; do
    if [[ "$key" == $'\x12' ]]; then  # Ctrl+R (ASCII 18)
        reload_cli
    elif [[ "$key" == $'\x1b' ]]; then  # ESC key
        read -r -n1 -s
        if [[ $REPLY == '[' ]]; then
            read -r -n1 -s
            case $REPLY in
                'A') send_to_other_panes "Up"    ;; # Up arrow
                'B') send_to_other_panes "Down"  ;; # Down arrow
                'C') send_to_other_panes "Right" ;; # Right arrow
                'D') send_to_other_panes "Left"  ;; # Left arrow
                '5')
                    read -r -n1 -s
                    if [[ $REPLY == '~' ]]; then
                        send_to_other_panes "PPage" # Page Up
                    fi
                    ;;
                '6')
                    read -r -n1 -s
                    if [[ $REPLY == '~' ]]; then
                        send_to_other_panes "NPage" # Page Down
                    fi
                    ;;
            esac
        else
            send_to_other_panes "$key$REPLY"
        fi
    elif [[ "$key" == "s" ]]; then
        send_to_other_panes "Enter"
    elif [[ "$key" == $'\x0a' ]] || [[ "$key" == $'\x0d' ]]; then  # Enter key (LF or CR)
        send_to_other_panes "Enter"
    else
        send_to_other_panes "$key"
    fi
done < /dev/tty