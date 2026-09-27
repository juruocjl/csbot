FROM registry-1.docker.io/library/python:3.12-slim-bookworm@sha256:392307d22300de8b5986851a12d9176dfc0fc073e65bf6523ebd7dcbeb23564e
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 MPLCONFIGDIR=/tmp/matplotlib
RUN pip install --no-cache-dir numpy==2.2.6 matplotlib==3.10.6 pillow==11.3.0
COPY csdata.py /opt/csdata.py
ENV PYTHONPATH=/opt
USER 65534:65534
WORKDIR /work
ENTRYPOINT ["python", "-u", "-c", "import sys; exec(compile(sys.stdin.read(), '<agent-script>', 'exec'))"]
