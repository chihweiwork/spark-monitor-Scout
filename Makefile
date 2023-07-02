VENV           = ./venv
BIN            = $(VENV)/bin
VENV_PYTHON    = $(BIN)/python
SYSTEM_PYTHON  = $(or $(shell which python3), $(shell which python))
PYTHON         = $(or $(wildcard $(VENV_PYTHON)), $(SYSTEM_PYTHON))
MYPATH         = $(PWD)/lib:$(PATH)

venv: ## Create python venv and update pip
	$(SYSTEM_PYTHON) -m venv $(VENV)
	$(VENV_PYTHON) -m pip install --upgrade pip
	$(VENV_PYTHON) -m pip install -U setuptools wheel
install: ## Install python package with requirement.txt
	$(VENV_PYTHON) -m pip install -r requirements.txt

clean-venv: ## remove venv
	rm -rf venv
clean-build: ## remove target create by pyinstaller
	rm -rf dist .tox *.egg-info 
clean-env: ## remove other thing
	rm -rf __pycache__ logs

pip-freeze: ## get python requirement
	. ./venv/bin/activate
	pip freeze > requirements.txt

all: venv install ## install this project
clean: clean-venv clean-build clean-env ## clean all
rebuild: clean-venv clean-build clean-env venv install ## rebuild this project

help: ## print help
	@egrep -h '\s##\s' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m  %-30s\033[0m %s\n", $$1, $$2}'

.PHONY: clean venv install rebuild
