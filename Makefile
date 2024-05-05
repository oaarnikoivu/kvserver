check:
	@poetry run ruff check kvserver/

format:
	@poetry run ruff format kvserver/

run:
	@poetry run python kvserver/main.py