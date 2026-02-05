import os
import pandas as pd
import yaml

from models.regression_engine import run_both_models
from pipelines.ieso_pipeline import build_ieso_master
from pipelines.aeso_pipeline import build_aeso_master

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class UniversalPipeline:
    def __init__(self):
        self.config_path = os.path.join(BASE_DIR, "config.yaml")
        with open(self.config_path, "r", encoding="utf-8") as f:
            self.config = yaml.safe_load(f)

        self.output_base = os.path.join(BASE_DIR, "output")
        os.makedirs(self.output_base, exist_ok=True)

    def _get_paths(self, market: str):
        market_dir = os.path.join(self.output_base, market)
        os.makedirs(market_dir, exist_ok=True)
        return {"market_output_dir": market_dir}

    def run_market(
        self,
        market: str,
        city: str,
        upload_mode: str | None = None,
        file_format: str | None = None,
        files=None,
    ):
        # ---------------- UPLOAD MARKET ----------------
        if market == "upload":
            from pipelines.user_pipeline import build_user_master

            paths = self._get_paths("upload")
            market_output_dir = paths["market_output_dir"]

            master_path = build_user_master(
                upload_mode=upload_mode,
                file_format=file_format,
                files=files,
                output_dir=market_output_dir,
                city=city,
                timezone="UTC",
            )

        else:
            # ---------------- IESO / AESO ----------------
            paths = self._get_paths(market)
            market_output_dir = paths["market_output_dir"]

            if market == "ieso":
                xml_dir = self.config["markets"]["ieso"]["xml_dir"]

                master_path = build_ieso_master(
                    xml_dir=xml_dir,
                    output_dir=market_output_dir,
                    city=city,
                    timezone="UTC",
                )

            elif market == "aeso":
                input_dir = self.config["markets"]["aeso"]["csv_dir"]

                master_path = build_aeso_master(
                    input_dir=input_dir,
                    output_dir=market_output_dir,
                    city=city,
                    timezone="UTC",
                )

            else:
                raise ValueError(f"Unknown market: {market}")

        # ---------------- MODEL-READY CSVs ----------------
        df = pd.read_csv(master_path)
        df["timestamp"] = pd.to_datetime(df["timestamp"])

        wind_cols = ["timestamp", "Wind", "temperature", "wind_speed", "wind_direction"]
        solar_cols = ["timestamp", "Solar", "temperature", "cloud_cover"]

        wind_df = df.dropna(subset=["Wind"])[wind_cols]
        solar_df = df.dropna(subset=["Solar"])[solar_cols]

        wind_csv_path = os.path.join(market_output_dir, "wind_model_data.csv")
        solar_csv_path = os.path.join(market_output_dir, "solar_model_data.csv")

        wind_df.to_csv(wind_csv_path, index=False)
        solar_df.to_csv(solar_csv_path, index=False)

        # ---------------- RUN MODELS ----------------
        wind_results = run_both_models(
            csv_path=wind_csv_path,
            target="Wind",
            features=["temperature", "wind_speed", "wind_direction"],
            label=f"{market.upper()}_Wind",
        )

        solar_results = run_both_models(
            csv_path=solar_csv_path,
            target="Solar",
            features=["temperature", "cloud_cover"],
            label=f"{market.upper()}_Solar",
        )

        return {
            "market": market,
            "city": city,
            "wind": wind_results,
            "solar": solar_results,
            "master_path": master_path,
            "wind_csv": wind_csv_path,
            "solar_csv": solar_csv_path,
        }
