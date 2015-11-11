#!/bin/bash
argparse(){
    argparser=$(mktemp)
    cat > "$argparser" <<EOF
from __future__ import print_function
import sys
import argparse
import os
class MyArgumentParser(argparse.ArgumentParser):
    def print_help(self, file=None):
        """Print help and exit with error"""
        super(MyArgumentParser, self).print_help(file=file)
        sys.exit(1)
parser = MyArgumentParser(prog=os.path.basename("$0"))
EOF

    # stdin to this function should contain the parser definition
    cat >> "$argparser"

    cat >> "$argparser" <<EOF
# args = parser.parse_args()
for arg in [a for a in dir(args) if not a.startswith('_')]:
    value = getattr(args, arg, None)
    if value is None:
        value = ''
    print('{}="{}";'.format(arg, value))
EOF

    # Define variables corresponding to the options if the args can be
    # parsed without errors; otherwise, print the text of the error
    # message.
    if python "$argparser" "$@" &> /dev/null; then
        eval $(python "$argparser" "$@")
        retval=0
    else
        python "$argparser" "$@"
        retval=1
    fi

    rm "$argparser"
    return $retval

}

if echo $0 | grep -q argparse.bash; then
    cat <<FOO
#!/bin/bash
source \$(dirname \$0)/argparse.bash || exit 1
argparse "\$@" <<EOF || exit 1
parser.add_argument('infile')
parser.add_argument('-o', '--outfile')
EOF
echo "INFILE: \${INFILE}"
echo "OUTFILE: \${OUTFILE}"
FOO
fi


argparse "$@" <<EOF || exit 1
import os
import errno
import os.path
parser.add_argument("--host", default="127.0.0.1", type=str)
parser.add_argument("--port", required=True, type=int)
parser.add_argument("--maxmemory", default="", type=str)
parser.add_argument("--data", required=True, type=str)
parser.add_argument("--log", required=True, type=str)
args = parser.parse_args()
args.data = os.path.abspath(args.data)
args.log = os.path.abspath(args.log)
for path in [os.path.join(args.data, str(args.port)), args.log]:
    if not os.path.exists(path):
        try:
            os.makedirs(path)
        except OSError as exc:
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                raise
EOF
echo $data
echo $log

docker run --user="$(id -u):$(id -g)" \
           -p $host:$port:$port \
           -v $data:/opt/data \
           -v $log:/opt/log \
           -d redis-nds:lastest \
           "redis-server" "--port" "$port" \
                          "--logfile" "/opt/log/$port.log" \
                          "--dir" "/opt/data/$port" \
                          "--bind" "0.0.0.0" \
                          "--maxmemory" "$maxmemory" \
                          "--appendonly" "yes" \
                          "--nds" "yes" \
                          "--nds-keycache" "yes" \
