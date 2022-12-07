ISTIO_VERSION ?= 1.16.0

.PHONY: default
default:
	go vet ./...
	go build -ldflags "-X main.Version=$(ISTIO_VERSION)" -o _output/istio-upgrade-$(ISTIO_VERSION)

.PHONY: release
release: update-go-mod default

.PHONY: update-go-mod
update-go-mod:
	go get istio.io/istio@$(ISTIO_VERSION)
	go get istio.io/api@$(ISTIO_VERSION)
	go get istio.io/pkg@$(ISTIO_VERSION)
