tqf:
	go test ./queue/

tqb:
	go test ./queue/ -bench=. -benchmem

tq: tqf tqb

build:
	cd server && CGO_ENABLED=0 go build -o ../app/app