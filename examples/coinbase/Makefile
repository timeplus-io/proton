

PWD = $(shell pwd)
NAME = test

create:	
	yq -o=json $(PWD)/pipeline.yaml | curl http://localhost:4195/streams/$(NAME) -X POST  -d  @- 

delete:
	curl http://localhost:4195/streams/$(NAME) -X DELETE
