docker run \
	--interactive --tty \
	--rm \
	--volume "$(pwd)/logs":/app/logs \
	--volume "$(pwd)/gunicorn.conf.py":/app/gunicorn.conf.py \
	--volume "$(pwd)/config.toml":/app/config.toml \
	--publish 8000:8000 \
	"$@"
