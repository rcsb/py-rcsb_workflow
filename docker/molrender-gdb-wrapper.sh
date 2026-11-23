#!/bin/bash
# TEST ONLY: installed in place of molrender.js so the existing imgs_exec_cli call is unchanged.
# Runs the real molrender under gdb. On a crash it prints a native backtrace to stderr (which
# imgs_exec_cli logs on failure) and exits 139; otherwise it passes through output and exit code.
REAL=/opt/modules/node_modules/molrender/build/bin/molrender-real.js
LOG=$(mktemp)

gdb -q -batch -nx \
    -ex "set pagination off" \
    -ex "set disable-randomization off" \
    -ex "set startup-with-shell off" \
    -ex "set print thread-events off" \
    -ex "handle SIGPIPE nostop noprint pass" \
    -ex "run" \
    -ex "echo \n=== CRASH: signal/backtrace of crashing thread ===\n" \
    -ex "info program" \
    -ex "bt 60" \
    -ex "x/3i \$pc" \
    -ex "echo \n=== CRASH: all threads (top 12 frames) ===\n" \
    -ex "thread apply all bt 12" \
    --args /usr/local/bin/node "$REAL" "$@" >"$LOG" 2>&1

if grep -qE "received signal SIG(SEGV|BUS|ABRT|ILL|FPE)" "$LOG"; then
    cat "$LOG" >&2
    rm -f "$LOG"
    exit 139
fi

cat "$LOG"
if grep -q "\[Inferior 1 (process [0-9]*) exited normally\]" "$LOG"; then
    code=0
else
    # gdb prints the exit code in octal; if gdb never ran the program, fail with 1
    code=$(sed -nE 's/.*\[Inferior 1 \(process [0-9]+\) exited with code ([0-7]+)\].*/\1/p' "$LOG" | tail -1)
    code=$((8#${code:-1}))
fi
rm -f "$LOG"
exit "$code"
