gen_http_client: kill_node
	echo "Running demo server..."
	cargo run --features swagger --bin reactor_nctrl -- --port 3000 /tmp &
	SERVER_PID=$!
	sleep 5
	echo "Generating client"
	docker run --rm --network=host -v $PWD:/local -u $(id -u):$(id -g)  \
		openapitools/openapi-generator-cli generate -i http://host.docker.internal:3000/api-doc/openapi.json \
		-g rust -o /local/rpc_client/ --additional-properties=packageName=reactor-client
	kill ${SERVER_PID}

# Expects openapi-generator-cli to be installed:
# npm install @openapitools/openapi-generator-cli -g
gen_http_client_manual: kill_node
	@echo "Running demo server..."
	cargo run --features swagger --bin reactor_nctrl -- --port 3000 /tmp &
	sleep 5
	@echo "Generating client"
	openapi-generator-cli generate -i http://localhost:3000/api-doc/openapi.json \
	-g rust -o rpc_client/ --additional-properties=packageName=reactor-client

kill_node:
	@echo "Killing process on port 3000 if any..."
	@lsof -ti :3000 | xargs --no-run-if-empty kill

node: kill_node
	@echo "Running demo server..."
	cargo run --features swagger --bin reactor_nctrl -- --port 3000 /tmp