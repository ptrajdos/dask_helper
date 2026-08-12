ROOTDIR=$(realpath $(dir $(firstword $(MAKEFILE_LIST))))
UV := uv

SRCDIR=${ROOTDIR}/dask_helper
TESTDIR=${ROOTDIR}/tests
COVDIR=${ROOTDIR}/htmlcov_p
TOXDIR=${ROOTDIR}/.tox
COVERAGERC=${ROOTDIR}/.coveragerc
INSTALL_LOG_FILE=${ROOTDIR}/install.log
VENV_SUBDIR=${ROOTDIR}/venv
DOCS_DIR=${ROOTDIR}/docs


COVERAGE = coverage
UNITTEST_PARALLEL = unittest-parallel
PDOC= pdoc3
PYTHON=python
SYSPYTHON ?= python
PIP=pip
PYTEST=pytest
TOX=tox
TEE=tee
TOX_CORES=auto


LOGDIR=${ROOTDIR}/testlogs
LOGFILE=${LOGDIR}/`date +'%y-%m-%d_%H-%M-%S'`.log


.PHONY: clean

uv_install:
	$(SYSPYTHON) -m pip install --upgrade pip
	$(SYSPYTHON) -m pip install uv
# Default lock (PyPI)
uv.lock: uv_install
	$(UV) lock --index-strategy first-index

# Default install
pypackages: uv.lock
	RUST_LOG=debug $(UV) sync --extra test 2>&1 | $(TEE) $(INSTALL_LOG_FILE)
	touch $@

test: pypackages
	mkdir -p ${LOGDIR}  
	$(UV) run ${COVERAGE} run --branch  --source=${SRCDIR} -m unittest discover -p '*_test.py' -v -s ${TESTDIR} 2>&1 |tee -a ${LOGFILE}
	$(UV) run ${COVERAGE} html --show-contexts


test_parallel: pypackages
	mkdir -p ${COVDIR} ${LOGDIR}
	$(UV) run ${UNITTEST_PARALLEL} -j 0 --level test --disable-process-pooling -v -t ${ROOTDIR} -s ${TESTDIR} -p '*_test.py' --coverage --coverage-rcfile ./.coveragerc --coverage-source ${SRCDIR} --coverage-html ${COVDIR}  2>&1 |tee -a ${LOGFILE}

docs: pypackages
	$(UV) run $(PDOC) --force --html ${SRCDIR} --output-dir ${DOCS_DIR}

profile: pypackages
	
	$(UV) run ${PYTEST} -n auto --cov-report=html --cov=${SRCDIR} --profile ${TESTDIR}

tox_check: pypackages
	$(UV) run ${TOX} -p ${TOX_CORES} 

clean: clean_tox clean_covs
	rm -rf .venv uv.lock ${INSTALL_LOG_FILE} ${LOGDIR} ${COVDIR} ${DOCS_DIR}

clean_tox:
	rm -rf ${TOXDIR}

clean_covs:
	rm -rf ${ROOTDIR}/.coverage.*

RAY_PORT ?= 6379
RAY_DASHBOARD_PORT ?= 8265
RAY_NUM_CPUS ?= 4
IP ?= 127.0.0.1
RAY_MEMORY_GB ?= 10
RAY_OBJECT_STORE_GB ?= 2

# Start Dask head node (manager)
dask-head: pypackages
	$(UV) run dask-head --ip=$(IP) --port=$(RAY_PORT) --dashboard-port=$(RAY_DASHBOARD_PORT) --num-cpus=$(RAY_NUM_CPUS) --memory-gb=$(RAY_MEMORY_GB) --object-store-gb=$(RAY_OBJECT_STORE_GB)

# Start Dask worker node
# Usage: make dask-worker IP=<head-node-ip>
dask-worker: pypackages
	$(UV) run dask-worker --ip=$(IP) --port=$(RAY_PORT) --num-cpus=$(RAY_NUM_CPUS) --memory-gb=$(RAY_MEMORY_GB) --object-store-gb=$(RAY_OBJECT_STORE_GB)

dask-stop: pypackages
	$(UV) run dask-stop