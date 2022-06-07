# Applying or updating license headers

To add or update license headers in this or other Open Data Cube
projects, you can do the following:

Download the tool from [https://github.com/johann-petrak/licenseheaders](https://github.com/johann-petrak/licenseheaders) and make it executable.

```bash
wget https://raw.githubusercontent.com/johann-petrak/licenseheaders/master/licenseheaders.py
chmod +x licenseheaders.py
```

Change the sections on `python` files, to remove the `headerStartLine` and
`headerEndLine`, like:

```python
        "headerStartLine": "",
        "headerEndLine": "",
```

Run the tool:

```bash
python3 ./licenseheaders.py --tmpl license-template.txt --years 2015-2020 --ext py --dir datacube
python3 ./licenseheaders.py --tmpl license-template.txt --years 2015-2020 --ext py --dir integration_tests
python3 ./licenseheaders.py --tmpl license-template.txt --years 2015-2020 --ext py --dir tests
python3 ./licenseheaders.py --tmpl license-template.txt --years 2015-2020 --ext py --dir docs
python3 ./licenseheaders.py --tmpl license-template.txt --years 2015-2020 --ext py --dir examples
```
