source ../../venv/bin/activate
rm -rf ./log/*
pipenv run python3.8 dstest.py 3A 3B -n 200 -p 2 -o ./log/
