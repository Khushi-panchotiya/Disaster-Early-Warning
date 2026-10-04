PY := venv/bin/python

.PHONY: setup test run baseline spike reset stream detect dashboard ingest
setup:      ## create venv and install dependencies
	python3.11 -m venv venv && venv/bin/pip install -r requirements.txt
test:       ## run the test-suite (needs MongoDB running)
	$(PY) -m pytest tests -q
run:        ## start streaming job + detector + dashboard
	scripts/run_all.sh
baseline:   ## inject 60 min of quiet background chatter
	scripts/demo.sh baseline
spike:      ## inject a burst: make spike REGION=Assam KW=flood N=40
	scripts/demo.sh spike $(or $(REGION),Assam) $(or $(KW),flood) $(or $(N),40)
reset:      ## wipe all data
	scripts/demo.sh reset
stream:
	$(PY) -m streaming.stream_job
detect:
	$(PY) -m detector.detector
dashboard:
	venv/bin/streamlit run dashboard/app.py
ingest:     ## live Reddit ingestion (needs .env credentials)
	$(PY) -m ingestion.ingest
