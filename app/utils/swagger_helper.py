class SwaggerHelper:
    """
    Centralized Swagger metadata helper.
    Ensures consistent documentation across all APIs.
    """

    # ---------- INGESTION APIs ----------

    GAS_QUALITY = {
        "summary": "National Gas Quality",
        "description": "Triggers ingestion of National Gas Quality data for a given date range and optional site filters."
    }

    ENTSOG = {
        "summary": "ENTSOG Ingestion",
        "description": "Ingests ENTSOG operational gas flow data with optional filters such as operators, points, and indicators."
    }

    INSTANTANEOUS_FLOW = {
        "summary": "National Gas Instantaneous Flow",
        "description": "Triggers ingestion of instantaneous gas flow data from National Gas systems."
    }

    PUBLICATION_CATALOGUE = {
        "summary": "National Gas Publication Catalogue",
        "description": "Fetches the available National Gas publication catalogue to identify publication IDs."
    }

    GAS_PUBLICATIONS = {
        "summary": "National Gas Operational Data",
        "description": "Ingests National Gas operational publications for specified publication IDs and date range."
    }

    BMRS_FUELHH = {
        "summary": "BMRS Fuel",
        "description": "Ingests half-hourly fuel mix generation data from BMRS."
    }

    BMRS_DEMAND = {
        "summary": "BMRS Demand Outturn",
        "description": "Ingests electricity demand outturn data from BMRS."
    }

    AGSI = {
        "summary": "GIE - AGSI",
        "description": "Ingests AGSI gas storage data optionally filtered by country."
    }

    ALSI = {
        "summary": "GIE - ALSI",
        "description": "Ingests ALSI LNG storage data optionally filtered by country."
    }

    GIE_DATA = {
        "summary": "List GIE Dataset",
        "description": "Query processed AGSI/ALSI datasets with optional filters such as country, variable, and date range."
    }

    # ---------- DISCOVERY APIs ----------

    DATASETS = {
        "summary": "List Available Datasets",
        "description": "Returns all available dataset IDs stored in the raw ingestion layer."
    }

    FIELDS = {
        "summary": "Dataset Field Metadata",
        "description": "Returns schema-like metadata describing fields available in a dataset."
    }

    SAMPLE = {
        "summary": "Sample Dataset Records",
        "description": "Returns a small sample of raw ingested records for inspection."
    }

    RAW = {
        "summary": "Raw Dataset Preview",
        "description": "Preview raw JSON payloads stored in the ingestion layer with optional filters."
    }