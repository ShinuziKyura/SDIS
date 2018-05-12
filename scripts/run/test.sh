cd "${BASH_SOURCE%/*}/../dbs/bin"
if [ "$#" -eq 4 ]; then
    java dbs.peer.test.PeerTest $1 $2 $3 $4
elif [ "$#" -eq 3 ]; then
    java dbs.peer.test.PeerTest $1 $2 $3
elif [ "$#" -eq 2 ]; then
    java dbs.peer.test.PeerTest $1 $2
fi
