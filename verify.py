#!/usr/bin/env python3
import sys, importlib.util, pathlib
print("Python:", sys.version)
print("\\nsys.path:")
for p in sys.path:
    print("  ", p)
spec = importlib.util.find_spec("florida_property_scraper")
print("\\nfind_spec('florida_property_scraper'):", spec)
app_path = pathlib.Path("/app/florida_property_scraper")
print("\\nExists /app/florida_property_scraper:", app_path.exists())
if app_path.exists():
    print("Files in package folder:")
    for p in sorted(app_path.iterdir()):
        print("  ", p.name)
if spec:
    try:
        mod = importlib.import_module("florida_property_scraper")
        print("\\nImported module:", getattr(mod, "__name__", None))
        print("module file:", getattr(mod, "__file__", None))
    except Exception as e:
        print("Import raised an exception:", e)
else:
    print("\\nModule not importable. Check package layout or setup.py/pyproject.")
