# syntax=docker/dockerfile:1
FROM amsterdam/gob_wheelhouse:3.9-slim-bullseye as wheelhouse
MAINTAINER datapunt@amsterdam.nl


# Application stage.
FROM amsterdam/gob_baseimage:3.9-slim-bullseye as application
MAINTAINER datapunt@amsterdam.nl
# GOB base image: SQL Server driver.

# Fill the wheelhouse.
COPY --from=wheelhouse /opt/wheelhouse /opt/wheelhouse

# Install gobeventconsumer in /app folder.
WORKDIR /app

# Install required Python packages.
COPY requirements.txt /app/
RUN LIBGDAL_VERSION=$(gdal-config --version) pip install --no-cache-dir \
	--find-links /opt/wheelhouse --requirement requirements.txt
RUN rm requirements.txt
# Wheelhouse cleanup.
RUN rm -rf /opt/wheelhouse

RUN git clone https://github.com/Amsterdam/amsterdam-schema.git /tmp/ams-schema

# Copy gobeventconsumer module.
COPY gobeventconsumer gobeventconsumer

USER datapunt
CMD ["python", "-m", "gobeventconsumer"]


# Test.
FROM application as test
USER root

# Copy test module and tests.
COPY test.sh pyproject.toml ./
COPY tests tests

# Copy Jenkins files.
COPY .jenkins /.jenkins

USER datapunt
