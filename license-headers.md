# Applying or updating license headers

To add or update license headers in this or other Open Data Cube projects, you can do the following:

Ensure you have pre-commit hooks installed

```bash
pre-commit install
```

Run the `insert-license` hook:

```bash
pre-commit run insert-license --all-files
```

To remove the license headers, add the `--remove-header` arg to `.pre-commit-config.yaml` before running the hook.

To make updates to the license text, first remove the headers, then update `license-template.txt` before rerunning the hook as usual to add them back.

Note that the date range is automatically updated to include the current year.

See the full documentation here: [https://github.com/Lucas-C/pre-commit-hooks#insert-license](https://github.com/Lucas-C/pre-commit-hooks#insert-license)
