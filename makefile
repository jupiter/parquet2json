.PHONY: checks
checks:
	cargo update --locked
	cargo check
	cargo test
	cargo clippy --all-targets -- -D warnings
	cargo fmt -- --check
