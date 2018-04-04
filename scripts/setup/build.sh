mkdir "${BASH_SOURCE%/*}/../dbs/bin"
javac -d "${BASH_SOURCE%/*}/../dbs/bin" -cp "${BASH_SOURCE%/*}/../dbs/src" "${BASH_SOURCE%/*}/../dbs/src/dbs/peer/Peer.java"
javac -d "${BASH_SOURCE%/*}/../dbs/bin" -cp "${BASH_SOURCE%/*}/../dbs/src" "${BASH_SOURCE%/*}/../dbs/src/dbs/peer/test/PeerTest.java"
