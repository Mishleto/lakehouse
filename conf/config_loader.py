"""
config_loader.py
-----------------
Loads environment-specific config based on the LAKEHOUSE_ENV environment
variable (dev / test / prod, defaults to dev).

Usage:
    from conf.config_loader import CONFIG
    CONFIG["nessie"]["uri"]

Set the environment before running any pipeline:
    export LAKEHOUSE_ENV=dev    # or test, prod
"""

import os
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = PROJECT_ROOT / "config"


def load_config(env: str | None = None) -> dict:
    env = (env or os.environ.get("LAKEHOUSE_ENV", "dev")).lower()
    config_path = CONFIG_DIR / f"config.{env}.yaml"

    if not config_path.exists():
        raise FileNotFoundError(
            f"Config file not found: {config_path}\n"
            f"LAKEHOUSE_ENV='{env}' — expected config/config.{env}.yaml.\n"
            f"Copy config/config.example.yaml to config/config.{env}.yaml "
            f"and fill in real values."
        )

    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    cfg["_env"] = env
    return cfg


CONFIG = load_config()
