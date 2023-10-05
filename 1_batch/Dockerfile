FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

ARG WORKDIR=/template
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

ARG PYTHON_PY_FILE=e_commerce_batch.py

COPY . .

ENV PYTHONPATH ${WORKDIR}

ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/${PYTHON_PY_FILE}"
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="${WORKDIR}/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE="${WORKDIR}/setup.py"

# We could get rid of installing libffi-dev and git, or we could leave them.
RUN apt-get update \
    # Upgrade pip and install the requirements.
    && pip install --upgrade pip \
    && pip install --no-cache-dir --upgrade pip \
    && pip download --dest /tmp/dataflow-requirements-cache -r $FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE

# Since we already downloaded all the dependencies, there's no need to rebuild everything.
ENV PIP_NO_DEPS=True

ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]
