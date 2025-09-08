# How to use Reactor.


## Initial Setup

1. Start new Rust project:- `cargo init --lib my_proj`
2. Modify `Cargo.toml` to contain
```toml
[lib]
crate-type = ["cdylib", "rlib"] 
```
3. Add reactor as the dependency.
`cargo add --git https://github.com/satyamjay-iitd/reactor/ reactor-actor`
4. Define some actors.
5. Compile the library:-
```bash
cargo build
# You should now have a `target/debug/libmy_proj.so`, and ready to run a job.
```


## Running a job

1. Install node controller.
`cargo install --git https://github.com/satyamjay-iitd/reactor/ reactor_nctrl`
2. Install job controller.
`cargo install --git https://github.com/satyamjay-iitd/reactor/ reactor_jctrl`
3. Start the node controller
```bash
# Note that the directory must contain the *.so file.
reactor_nctrl --port 3000 ./target/debug`
```
4. Define job in a toml file.
```toml
[[ops]]
name = "pingpong"
lib_name = "ping_pong_actor"

[[nodes]]
name = "node1"
hostname = "0.0.0.0"
port = 3000

[placement]
  [[placement.pingpong]]
  nodename = "node1"
  actor_name = "pinger"
  other = "ponger"

  [[placement.pingpong]]
  nodename = "node1"
  actor_name = "ponger"
  other = "pinger"
```
5. Run the job.
```bash
reactor_jctrl ./job.toml
```
