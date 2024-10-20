FROM rust:1.80-alpine as builder

RUN apk add --no-cache musl-dev

WORKDIR /usr/src/app
COPY . .
RUN cargo build --release

FROM alpine:3.14
COPY --from=builder /usr/src/app/target/release/shadow_proxy /shadow_proxy
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
ENV RUST_LOG=info
EXPOSE 8080
RUN chmod +x /shadow_proxy
CMD ["/shadow_proxy"]