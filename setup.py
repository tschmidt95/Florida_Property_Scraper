from setuptools import setup, find_packages

def parse_requirements(fname):
    with open(fname, 'r') as fh:
        lines = [ln.strip() for ln in fh.readlines()]
    # keep non-empty, non-comment lines
    return [ln for ln in lines if ln and not ln.startswith('#')]

requirements = parse_requirements('requirements.txt')

setup(
    name="florida_property_scraper",
    version="0.1.0",
    packages=find_packages(
        where="src",
        exclude=(
            "florida_property_scraper.comps",
            "florida_property_scraper.comps.*",
        ),
    ),
    package_dir={"": "src"},
    include_package_data=True,
    install_requires=requirements,
    extras_require={"test": ["pytest", "responses", "httpx>=0.27"]},
    entry_points={
        "console_scripts": [
            "florida_property_scraper=florida_property_scraper.__main__:main",
            "florida-property-scraper=florida_property_scraper.__main__:main",
        ]
    },
)
