import os
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from sklearn.linear_model import LinearRegression, HuberRegressor
from sklearn.preprocessing import PolynomialFeatures
from sklearn.pipeline import Pipeline
from sklearn.metrics import r2_score


def train_test_split_by_time(df, target, test_days=183):
    df = df.sort_values("timestamp")
    cutoff = df["timestamp"].max() - pd.Timedelta(days=test_days)
    train = df[df["timestamp"] <= cutoff]
    test = df[df["timestamp"] > cutoff]
    return train, test


def add_lags(df, target, lags=(1,)):
    df = df.copy()
    for l in lags:
        df[f"lag{l}"] = df[target].shift(l)
    return df


def build_equation(model, feature_names):
    coef = model.coef_
    intercept = model.intercept_

    terms = [f"{intercept:.3f}"]
    for c, f in zip(coef, feature_names):
        terms.append(f"{c:.3f}*{f}")

    return " + ".join(terms)


def save_plots(prefix, target, y_test, y_pred, equation, r2_val):
    os.makedirs("output/plots", exist_ok=True)

    # Scatter plot
    plt.figure(figsize=(10, 8))
    plt.scatter(y_test, y_pred, alpha=0.5)
    plt.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'k--')
    plt.xlabel(f"Actual {target}")
    plt.ylabel(f"Predicted {target}")
    plt.title(f"{prefix} — Actual vs Predicted")
    plt.text(
        0.01, 0.99,
        f"{equation}\nR² = {r2_val:.4f}",
        transform=plt.gca().transAxes,
        fontsize=9,
        color="darkred",
        va="top"
    )
    plt.tight_layout()
    scatter_path = f"output/plots/{prefix.lower()}_scatter.png"
    plt.savefig(scatter_path, dpi=150)
    plt.close()

    # Time series plot
    plt.figure(figsize=(14, 6))
    plt.plot(y_test, label="Actual")
    plt.plot(y_pred, '--', label="Predicted")
    plt.title(f"{prefix} — Test Set Time Series")
    plt.xlabel("Hour Index")
    plt.ylabel(target)
    plt.legend()
    plt.tight_layout()
    ts_path = f"output/plots/{prefix.lower()}_timeseries.png"
    plt.savefig(ts_path, dpi=150)
    plt.close()

    return scatter_path, ts_path


def run_both_models(csv_path, target, features, label, test_days=183):
    df = pd.read_csv(csv_path)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.dropna(subset=[target] + features)

    # ---------------- Linear ----------------
    df_lin = add_lags(df, target, lags=(1,))
    df_lin = df_lin.dropna()
    features_lin = features + ["lag1"]

    train_lin, test_lin = train_test_split_by_time(df_lin, target, test_days)
    X_train_lin = train_lin[features_lin].to_numpy()
    y_train_lin = train_lin[target].to_numpy()
    X_test_lin = test_lin[features_lin].to_numpy()
    y_test_lin = test_lin[target].to_numpy()

    lin_model = LinearRegression()
    lin_model.fit(X_train_lin, y_train_lin)
    y_pred_lin = lin_model.predict(X_test_lin)
    r2_lin = r2_score(y_test_lin, y_pred_lin)
    eq_lin = build_equation(lin_model, features_lin)
    scatter_lin, ts_lin = save_plots(f"{label}_Linear", target, y_test_lin, y_pred_lin, eq_lin, r2_lin)

    # ---------------- Polynomial ----------------
    df_poly = add_lags(df, target, lags=(1, 2))
    df_poly = df_poly.dropna()
    features_poly = features + ["lag1", "lag2"]

    train_poly, test_poly = train_test_split_by_time(df_poly, target, test_days)
    X_train_poly = train_poly[features_poly].to_numpy()
    y_train_poly = train_poly[target].to_numpy()
    X_test_poly = test_poly[features_poly].to_numpy()
    y_test_poly = test_poly[target].to_numpy()

    poly_pipeline = Pipeline([
        ("poly", PolynomialFeatures(degree=2, include_bias=False)),
        ("huber", HuberRegressor())
    ])

    poly_pipeline.fit(X_train_poly, y_train_poly)
    y_pred_poly = poly_pipeline.predict(X_test_poly)
    r2_poly = r2_score(y_test_poly, y_pred_poly)

    poly_feature_names = poly_pipeline.named_steps["poly"].get_feature_names_out(features_poly)
    huber = poly_pipeline.named_steps["huber"]
    eq_poly = build_equation(huber, poly_feature_names)

    scatter_poly, ts_poly = save_plots(f"{label}_Poly", target, y_test_poly, y_pred_poly, eq_poly, r2_poly)

    # ---------------- Best model ----------------
    if r2_poly >= r2_lin:
        best = "polynomial"
        best_r2 = r2_poly
        best_eq = eq_poly
    else:
        best = "linear"
        best_r2 = r2_lin
        best_eq = eq_lin

    return {
        "linear": {
            "r2": r2_lin,
            "equation": eq_lin,
            "scatter_plot": scatter_lin,
            "timeseries_plot": ts_lin
        },
        "polynomial": {
            "r2": r2_poly,
            "equation": eq_poly,
            "scatter_plot": scatter_poly,
            "timeseries_plot": ts_poly
        },
        "best_model": best,
        "best_r2": best_r2,
        "best_equation": best_eq
    }
