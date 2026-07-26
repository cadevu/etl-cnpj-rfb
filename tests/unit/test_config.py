from etl_cnpj.config import Settings


def test_settings_paths_are_under_repo_root():
    settings = Settings()
    assert settings.bronze_dir.name == "bronze"
    assert settings.silver_dir.parent == settings.data_dir
