tqf:
	go test ./queue/

tqb:
	go test ./queue/ -bench=. -benchmem

tq: tqf tqb