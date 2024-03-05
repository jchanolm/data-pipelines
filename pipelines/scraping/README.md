# Modules

## Scraping
-----
The scraping module can be imported or run as a package using python -m.
It includes internal packages, each dedicated to a specific service 
and containing a scrape.py file to execute the scraping process for that service. 

Each service is initiated by running its `scrape.py` file.

Scrapers save data to `pipelines/scraping/$module_name/data/`

Define module name in the subclass.

```
scraping_module/
│
├── __init__.py
├── discourse/
│   ├── __init__.py
│   └── scrape.py
├── snapshot/
│   ├── __init__.py
│   └── scrape.py
└── ...
```

## Planned scrapers
---- 
- `discourse` [x]
- `snapshot` *in progress*
- `github` []
- `multisigs` []
- `gitcoin-grants-donors` []
- `dune-contributors` []
...
## Design strategy
------ 
- Each scraper inherits from `Scraper` class defined in `helpers/scraper.py`
- Each scraper must live in its own folder
- Each service must have a README.me file that defines the scraper's usage arguments
- Each service must explain how the scraper works and what data the scraper collects in its README.md file
- Each service must have an `__init__.py` file exporting the scraper and all other defined classes for use in the module
- Each scraper  a `scrape.py` file as its main executable
- Each scraper must save its data in its  `data/` subdirectory as a `.json`
- Each service must read and save its necessary metadata, such as last item scraped, in `data/scraper_metadata.json`

