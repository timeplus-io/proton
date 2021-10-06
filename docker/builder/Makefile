build: image
	mkdir -p $(HOME)/.ccache
	docker run --network=host --rm --workdir /server --volume $(realpath ../..):/server --cap-add=SYS_PTRACE --mount=type=bind,source=$(HOME)/.ccache,destination=/ccache -e CCACHE_DIR=/ccache -it timeplus/proton-builder

pull:
	docker pull timeplus/proton-builder

image:
	docker build --network=host -t timeplus/proton-builder .

image_push:
	docker push timeplus/proton-builder
