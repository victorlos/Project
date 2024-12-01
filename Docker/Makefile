
.PHONY: configure-aws
run configure-aws:
	docker run --rm -ti -v ~/.aws:/root/.aws amazon/aws-cli configure

.PHONY: aws-ls
run	aws-ls: configure-aws
	docker run --rm -ti -v ~/.aws:/root/.aws amazon/aws-cli s3 ls