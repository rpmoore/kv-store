build:
    cargo build
release:
    cargo build --release
clean:
    cargo clean
init:
    openssl genrsa -out key.pem 2048
    openssl rsa -in key.pem -pubout > key.pub