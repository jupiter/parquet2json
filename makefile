.PHONY: checks
checks:
	cargo check
	cargo test
	cargo clippy --all-targets -- -D warnings
	cargo fmt -- --check
