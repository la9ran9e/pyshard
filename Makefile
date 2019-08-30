.PHONY: test

PYTHON?=python
TEST_BIN_PATH?=./env/bin

testenv-start:
	@echo "[+] starting test env..."
	${PYTHON} ${TEST_BIN_PATH}/test_env.py build config_example.json

test:
	@echo "[+] starting tests..."
	coverage run -m unittest discover test || :

testenv-kill:
	@echo "[-] killing env..."
	${PYTHON} ${TEST_BIN_PATH}/test_env.py kill
	@echo "[-] done"
