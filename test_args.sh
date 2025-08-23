#!/bin/bash
echo "Args count: $#"
echo "Arg 1: '$1'"
echo "All args: '$@'"

# Also check if stdin is a terminal
if [[ -t 0 ]]; then
    echo "stdin is a terminal"
else
    echo "stdin is NOT a terminal (piped/redirected)"
fi

# Try to read from stdin
echo "Trying to read from stdin..."
read -t 1 -p "Enter something: " input
echo "Read: '$input'"