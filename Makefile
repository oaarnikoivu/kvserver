PORT ?= 8000

check:
	@poetry run ruff check kvserver/

format:
	@poetry run ruff format kvserver/

server:
	@poetry run python kvserver/server.py --port $(PORT)

client:
	@poetry run python kvserver/client.py --sport $(PORT)

.PHONY: check format server client