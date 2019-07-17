.PHONY: test

PYTHON?=python
TEST_BIN_PATH?=./env/bin

test:
	@echo "[+] starting test env..."
	${PYTHON} ${TEST_BIN_PATH}/test_env.py build config_example.json
	@echo "[+] starting tests..."
	${PYTHON} -m unittest discover test || :
	@echo "[-] killing env..."
	${PYTHON} ${TEST_BIN_PATH}/test_env.py kill
	@echo "[-] done"

