.PHONY: test tools help

ifdef j
JOBS := $(j)
else
JOBS := 1
endif

all:
	cd build && ./build.sh -n $(JOBS)

test:
	cd test && pytest -x --log-cli-level=INFO test_vearch.py

tools:
	for tools in $(shell find ./tools -mindepth 1 -maxdepth 1 -type d); do \
		(cd $$tools; echo Build $$tools; go mod tidy; go build) || exit 1; done

help:
	@echo '===================='
	@echo '-- DOCUMENTATION --'
	@echo 'all                          - build vearch executable file'
	@echo 'test                         - run test cases'
	@echo 'tools                        - build vearch tools'
