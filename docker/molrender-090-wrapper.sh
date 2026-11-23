#!/bin/bash
# TEMP: installed in place of molrender.js so the existing imgs_exec_cli call is unchanged.
# molrender 0.9.0 does not accept --jpeg-quality/--jpeg-chroma (added in 0.10.0), so drop them
# and pass everything else through to the real molrender.
args=()
while [ $# -gt 0 ]; do
    case "$1" in
        --jpeg-quality | --jpeg-chroma) shift 2 || shift ;;
        --jpeg-quality=* | --jpeg-chroma=*) shift ;;
        *)
            args+=("$1")
            shift
            ;;
    esac
done
exec node /opt/modules/node_modules/molrender/build/bin/molrender-real.js "${args[@]}"
