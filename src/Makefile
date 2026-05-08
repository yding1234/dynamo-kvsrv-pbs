# Dynamo-style KV + PBS project (module dynamo-kvsrv; MIT-origin labrpc/tester scaffolding retained).

.PHONY: test build daemon plot pbscmd

test: daemon
	go test ./...

build: daemon plot pbscmd

daemon:
	mkdir -p bin
	go build -o bin/kvsrv1d ./cmd/kvsrv1d

plot:
	mkdir -p bin
	go build -o bin/kvsrv1pbsplot ./cmd/kvsrv1pbsplot

pbscmd:
	mkdir -p bin
	go build -o bin/posterdeltakp ./cmd/posterdeltakp
	go build -o bin/pbsdroprate ./cmd/pbsdroprate
	go build -o bin/pbsnwr ./cmd/pbsnwr
	go build -o bin/pbsshortdelay ./cmd/pbsshortdelay
	go build -o bin/overlapwritesplot ./cmd/overlapwritesplot
