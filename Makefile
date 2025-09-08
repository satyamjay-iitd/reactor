gen_http_client: kill_node
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


kill_node:
	@echo "Killing process on port 3000 if any..."
	@lsof -ti :3000 | xargs --no-run-if-empty kill

node: kill_node
	@echo "Running demo server..."
	cargo run --features swagger --bin reactor_nctrl -- --port 3000 /tmp

generate:
	@echo "Generating client"
	openapi-generator-cli generate -i http://localhost:3000/api-doc/openapi.json \
		-g rust -o rpc_client/ --additional-properties=packageName=reactor-client

	openapi-generator-cli generate -i http://localhost:3000/api-doc/openapi.json \
		-g typescript-axios -o ./reactor-dashboard/api-client/src --additional-properties=packageName=reactor-client

