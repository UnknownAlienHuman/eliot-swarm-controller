#!/bin/sh
set -eu

mode=$1
cli=$2
data_dir=$3
agent=$4
generation=$5
other_command_id=$6
context_path=$7
own_command_path=$8
expected_cwd=
if [ "$#" -ge 9 ]; then
    expected_cwd=$9
fi

if [ -n "$expected_cwd" ]; then
    actual_cwd=$(pwd -P)
    [ "$actual_cwd" = "$expected_cwd" ] || exit 47
fi

[ "${CI-}" = "script-approved" ] || exit 40

# The controller bounds this one-line JSON input to 1 MiB. Keep its nonsecret bytes
# as a test artifact without relying on a JSON parser or an inherited PATH.
context=
if IFS= read -r context; then
    :
elif [ -z "$context" ]; then
    exit 41
fi
printf '%s' "$context" > "$context_path"

# This is deliberately the Run principal's first RPC. Capture stdout so it never
# mixes with the NDJSON action stream emitted below.
run_command_id=${ELIOT_RUN_COMMAND_ID-}
if own_output=$("$cli" --data-dir "$data_dir" --json cmd show "$run_command_id" 2>&1); then
    :
else
    own_status=$?
    printf 'ELIOT_RUN_OWN_COMMAND_EXIT=%s\n' "$own_status" >&2
    printf '%s' "$own_output" | /usr/bin/head -c 4096 >&2
    printf '\n' >&2
    exit 42
fi
printf '%s' "$own_output" > "$own_command_path"

# The trigger ID is a known Owner command; its read must fail for a scoped Run.
if other_output=$("$cli" --data-dir "$data_dir" --json cmd show "$other_command_id" 2>&1); then
    exit 43
else
    other_status=$?
fi
[ "$other_status" -eq 1 ] || exit 44
case "$other_output" in
    *not_allowed*) ;;
    *) exit 45 ;;
esac

case "$mode" in
    success)
        printf '{"action":"send","target":"%s","expected_generation":%s,"text":"script-event-send"}\n' "$agent" "$generation"
        ;;
    invalid)
        printf '{"action":"send","target":"%s","expected_generation":%s,"text":"must-not-dispatch"}\n' "$agent" "$generation"
        printf '{"action":"send","target":"%s","expected_generation":%s,"text":"must-not-dispatch","authority":"owner"}\n' "$agent" "$generation"
        ;;
    *) exit 46 ;;
esac
