FROM registry-1.docker.io/library/python:3.12-slim-bookworm@sha256:392307d22300de8b5986851a12d9176dfc0fc073e65bf6523ebd7dcbeb23564e
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 MPLCONFIGDIR=/tmp/matplotlib
ARG PIP_INDEX_URL=https://pypi.org/simple
RUN pip install --no-cache-dir numpy==2.2.6 matplotlib==3.10.6 pillow==11.3.0 \
    contourpy==1.4.0 cycler==0.12.1 fonttools==4.66.0 kiwisolver==1.5.1 \
    packaging==26.3 pyparsing==3.3.3 python-dateutil==2.9.0.post0 six==1.17.0
COPY csdata.py /opt/csdata.py
ENV PYTHONPATH=/opt
USER 65534:65534
WORKDIR /work
ENTRYPOINT ["python", "-u", "-c", "import sys; exec(compile(sys.stdin.read(), '<agent-script>', 'exec'))"]
