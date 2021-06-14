tqf:
	go test ./queue/

tqb:
	go test ./queue/ -bench=. -benchmem

tcompf:
	go test ./compress/

tq: tqf tqb

build:
	CGO_ENABLED=0 go build -o ./app/server.app ./server

build_encrypt_tool:
	CGO_ENABLED=0 go build -o ./app/encrypt_data_generator ./tools/encrypt_data_generator/

tool: build_encrypt_tool

ssl_gen:
	openssl req -x509 -newkey rsa:4096 -keyout ./app/key.pem -out ./app/cert.pem -days 3660 -nodes -subj '/CN=localhost'

cp_sc:
	cp config/stor.config.json app/
cp_cc:
	cp config/cluster.json tmp/
cp_ba:
	cp config/basic_auth.json tmp/
cp_autht:
	cp config/authorization.json tmp/

mkdt:
	mkdir -pv tmp

generate_encrypt:
	app/encrypt_data_generator -cfge "app/encrypt.json"

run:
	app/server.app -cfg "app/stor.config.json" -cfge "app/encrypt.json" -abfn "basic_auth.json" -arfn "authorization.json" -log_level trace
run_tls:
	app/server.app -cfg "app/stor.config.json" -cfge "app/encrypt.json" -abfn "basic_auth.json" -arfn "authorization.json" -tls_key "app/key.pem" -tls_cert "app/cert.pem" -log_level trace


br: build mkdt cp_sc cp_cc cp_ba cp_autht generate_encrypt run

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
e_hcsu:
	go run ./examples/108_create_and_start_handler_unload
e_hcsd:
	go run ./examples/109_create_and_start_handler_delete
e_hcsm:
	go run ./examples/110_create_and_start_handler_mark
e_hle:
	go run ./examples/111_get_handler_last_error

e_uac:
	go run ./examples/130_user_admin_create
e_utc:
	go run ./examples/131_user_tech_create

e_cg:
	go run ./examples/140_con_group

tbe: e_cq e_smtq e_gmfq e_chrs e_hrss e_cec e_chcu e_hcus e_hcsu e_hcsd e_hcsm e_hle e_uac e_utc e_cg

e_sns:
	go run ./examples/200_send_messages_to_queue_non_stop

e_g100:
	go run ./examples/201_get_100_messages_from_queue