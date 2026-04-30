# Analytical Platform Airflow Python Template

[![Ministry of Justice Repository Compliance Badge](https://github-community.service.justice.gov.uk/repository-standards/api/analytical-platform-airflow-cadet-deployer/badge)](https://github-community.service.justice.gov.uk/repository-standards/analytical-platform-airflow-cadet-deployer)

[![Open in Dev Container](https://raw.githubusercontent.com/ministryofjustice/.devcontainer/refs/heads/main/contrib/badge.svg)](https://vscode.dev/redirect?url=vscode://ms-vscode-remote.remote-containers/cloneInVolume?url=https://github.com/ministryofjustice/analytical-platform-airflow-cadet-deployer)

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/ministryofjustice/analytical-platform-airflow-cadet-deployer)

This repository contains the Airflow CaDeT image for use on the Analytical Platform.

## Scan the image with grype

Use `./scan-image-with-grype.sh` to run `grype` against a built image while keeping the scan summary intact and filtering the final results table down to `High` and `Critical` vulnerabilities only.

```bash
./scan-image-with-grype.sh analytical-platform-airflow-cadet-deployer:1.3.1
```

If you omit the image argument, the script defaults to `analytical-platform-airflow-cadet-deployer:1.3.1`. Any extra arguments are passed directly to `grype` after the built-in `--only-fixed -f high` flags.

Use `./scan-image-with-grype.py` if you want the same `High`/`Critical` filter but also want to collapse duplicate findings so only one row is kept per `(NAME, TYPE)` pair. The retained row is the one with the latest available `FIXED IN` version, with severity and risk used only as tie-breakers.

```bash
./scan-image-with-grype.py analytical-platform-airflow-cadet-deployer:1.3.1
```
