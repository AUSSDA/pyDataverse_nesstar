import os
from pydantic import BaseSettings


ROOT_DIR = os.path.dirname(os.path.dirname(__file__))


class Settings(BaseSettings):
    INSTANCE: str
    API_TOKEN: str
    DATA_DIRNAME: str
    DATASETS_PUBLISHED: bool = False
    SEPERATOR: str = "<s>"
    SIP_FOLDERNAME: str = "SIP"
    AIP_FOLDERNAME: str = "AIP"
    DIP_FOLDERNAME: str = "DIP"
    TERMS_OF_ACCESS_FILENAME = "terms-of-access.html"
    TERMS_OF_USE_FILENAME = "terms-of-use.html"


class DevelopmentSettings(Settings):
    BASE_URL: str = "https://dev.vdc.ac"
    DOI_PREFIX: str = "doi:10.5072"
    DATAVERSE_ALIAS: str = "gfk_test"

    class Config:
        env_file = os.path.join(ROOT_DIR, "env-configs/development.env")


class LocalhostT550Settings(Settings):
    BASE_URL: str = "http://localhost:8085"
    DOI_PREFIX: str = "doi:10.5072"
    DATAVERSE_ALIAS: str = "gfk_test"

    class Config:
        env_file = os.path.join(ROOT_DIR, "env-configs/localhost-t550.env")


class ProductionSettings(Settings):
    BASE_URL: str = "https://data.aussda.at"
    DOI_PREFIX: str = "doi:10.11587"
    DATAVERSE_ALIAS: str = "gfk"

    class Config:
        env_file = os.path.join(ROOT_DIR, "env-configs/production.env")


def get_settings(settings_name):
    settings = {
        "development": DevelopmentSettings(),
        "production": ProductionSettings(),
        "localhost-t550": LocalhostT550Settings(),
    }
    return settings[settings_name]
