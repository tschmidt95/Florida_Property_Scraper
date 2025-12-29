from setuptools import setup, find_packages
setup(
    name="florida_property_scraper",
    version="0.0.1",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[],
    entry_points={
        'console_scripts': [
            'florida_property_scraper=florida_property_scraper.__main__:main'
        ]
    }
)
