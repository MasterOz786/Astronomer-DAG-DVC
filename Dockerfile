FROM astrocrpublic.azurecr.io/runtime:3.1-5

# System packages are installed via packages.txt (Astronomer handles this)
# Build tools (gcc, g++, build-essential, python3-dev, libpq-dev) are listed there
# This ensures they're available when the ONBUILD installs Python dependencies

# Python dependencies will be installed automatically by ONBUILD from requirements.txt
# The packages.txt ensures build tools are available for compilation

COPY scripts/ /opt/airflow/scripts/

WORKDIR /opt/airflow