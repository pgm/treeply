import socket
import json

def main() :
    s = socket.socket(socket.AF_UNIX)
    s.connect("/tmp/treeply")

    s.send((json.dumps({"Type": "listdir", "Path": "."})+"\n").encode("utf8"))
    x = s.recv(10000)
    print(x)

if __name__ == "__main__":
    main()