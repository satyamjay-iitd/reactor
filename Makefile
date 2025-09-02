gen_http_client:
	echo "Running demo server..."
	- pkill reactor_nctrl

	cargo run --features swagger --bin reactor_nctrl -- --port 3000 /tmp &
	SERVER_PID=$!
	sleep 5

	echo "Generating client"
	openapi-generator-cli generate -i http://localhost:3000/api-doc/openapi.json \
		-g rust -o rpc_client/ --additional-properties=packageName=reactor-client

	openapi-generator-cli generate -i http://localhost:3000/api-doc/openapi.json \
		-g typescript-axios -o ./reactor-dashboard/api-client/src --additional-properties=packageName=reactor-client

	- pkill reactor_nctrl
