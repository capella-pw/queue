tqf:
	go test ./queue/

tqb:
	go test ./queue/ -bench=. -benchmem

tcompf:
	go test ./compress/

tq: tqf tqb

build:
	cd server && CGO_ENABLED=0 go build -o ../app/server.app

cp_sc:
	cp config/stor.config.json app/
cp_cc:
	cp config/cluster.json tmp/

mkdt:
	mkdir -pv tmp

run:
	app/server.app -cfg "app/stor.config.json" -log_level trace

br: build mkdt cp_sc cp_cc run

e_cq:
	go run ./examples/100_create_queue
e_smtq:
	go run ./examples/101_send_messages_to_queue
e_gmfq:
	go run ./examples/102_get_messages_from_queue
e_chrs:
	go run ./examples/103_create_handler_regularly_save
e_hrss:
	go run ./examples/104_handler_regularly_save_start
e_cec:
	go run ./examples/105_create_external_cluster
e_chcu:
	go run ./examples/106_create_handler_copy_unique
e_hcus:
	go run ./examples/107_handler_copy_unique_start

tbe: e_cq e_smtq e_gmfq e_chrs e_hrss e_cec e_chcu e_hcus