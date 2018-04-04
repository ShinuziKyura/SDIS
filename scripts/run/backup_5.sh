cd "${BASH_SOURCE%/*}/../dbs/bin"
java dbs.peer.test.PeerTest DBS_5_TEST BACKUP $1 $2
