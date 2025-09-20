GENERIC_NCTRL = ../../generic_nctrl
GENERIC_JCTRL = ../../generic_jctrl
TARGET_SO = ../../target/debug/lib_ab_actor.so

.PHONY: install install_node install_jobc kill_node node job clean TARGET_SO
	

install_node: $(GENERIC_NCTRL)
	cargo install --debug --path $(GENERIC_NCTRL)

install_jobc: $(GENERIC_JCTRL)
	cargo install --debug --path $(GENERIC_JCTRL)

kill_node:
	@echo "Killing process on port 3000 if any..."
	@lsof -ti :3000 | xargs --no-run-if-empty kill

node: kill_node install_node
	cargo build
	reactor_nctrl --port 3000 ../../target/debug
	
job: install_jobc
	reactor_jctrl ./ab.toml

clean:
	cargo uninstall reactor_nctrl || true
	cargo uninstall reactor_jctrl || true
	cargo clean	
