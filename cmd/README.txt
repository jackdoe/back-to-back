start the broker

$ go run main.go


start the echo http server
$ go run echo.go


start the reverse proxy server (edit proxy.conf if needed)
$ go run proxy.go -c proxy.conf


send command to the proxy on port 8081
$ curl -XPOST -d'{"test":"yey"}' -H "back-to-back-topic: abc" 'localhost:8081/some-path'


the command goes through the proxy, to the broker and then to the echo
server and back
