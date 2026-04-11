.PHONY: checks integration-test
checks:
	cargo update --locked
	cargo check
	cargo test
	cargo clippy --all-targets -- -D warnings
	cargo fmt -- --check

integration-test:
	cargo build --release
	./scripts/integration_test.sh
