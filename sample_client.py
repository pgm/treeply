import socket
import json

commands = {"listdir": [("Path", str)],
            "diag": [],
            "open": [("Path", str)],
            "close": [("FD", int)],
            "read": [("FD", int), ("Length", int)],
            "seek": [("FD", int), ("Offset", int), ("Whence", str)]}

def main() :
    s = socket.socket(socket.AF_UNIX)
    s.connect("/tmp/treeply")

    while True:
        command = input("Command: ")
        if command == "" or command.strip() == "quit":
            break
        command_parts = command.strip().split(" ")
        command_name = command_parts[0]
        param_types = commands[command_name]
        payload = {}
        for param_type, param in zip(param_types, command_parts[1:]):
            param_name, coersion = param_type
            payload[param_name] = coersion(param)
        
        msg = {"Type": command_name, "Payload": payload}
        s.send((json.dumps(msg)+"\n").encode("utf8"))
        x = s.recv(10000)
        print("Response: "+json.dumps(json.loads(x), indent=2))

if __name__ == "__main__":
    main()