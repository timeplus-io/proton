

start:
	docker run -d \
		--name proton \
		--pull always \
		-p 3218:3218 \
		-p 8463:8463 \
		d.timeplus.com/timeplus-io/proton:latest

stop:
	docker stop proton

cli:
	docker exec -it proton proton client