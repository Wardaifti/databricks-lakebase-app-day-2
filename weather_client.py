"""
weather_client.py — National Weather Service (api.weather.gov) client.

Mirrors massive_client.py's shape: a small client class that fetches raw
data and a normalizer that turns it into flat document records ready to
upsert into Lakebase (see ensure_weather_tables() / weather sync route
in app.py).

No API key required — NWS only asks for a descriptive User-Agent header.
"""

import hashlib
import os
from datetime import datetime, timezone

import requests

NWS_BASE_URL = os.environ.get("NWS_BASE_URL", "https://api.weather.gov")

# NWS has no geocoding endpoint — it needs lat/lon to resolve a grid point.
# Small built-in lookup for common bootcamp test cities; callers can also
# pass "lat,lon" directly (e.g. "41.8781,-87.6298") to bypass this table.
_CITY_COORDS = {
    "chicago, il": (41.8781, -87.6298),
    "austin, tx": (30.2672, -97.7431),
    "new york, ny": (40.7128, -74.0060),
    "los angeles, ca": (34.0522, -118.2437),
    "houston, tx": (29.7604, -95.3698),
    "miami, fl": (25.7617, -80.1918),
    "seattle, wa": (47.6062, -122.3321),
    "denver, co": (39.7392, -104.9903),
    "atlanta, ga": (33.7490, -84.3880),
    "boston, ma": (42.3601, -71.0589),
    "phoenix, az": (33.4484, -112.0740),
    "san francisco, ca": (37.7749, -122.4194),
    "dallas, tx": (32.7767, -96.7970),
    "philadelphia, pa": (39.9526, -75.1652),
    "minneapolis, mn": (44.9778, -93.2650),
    "new orleans, la": (29.9511, -90.0715),
    "oklahoma city, ok": (35.4676, -97.5164),
}


class WeatherClient:
    def __init__(self, base_url: str = NWS_BASE_URL):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "lakebase-weather-app (support@example.com)",
            "Accept": "application/geo+json",
        })

    # -- location resolution -------------------------------------------------
    def _resolve_latlon(self, location: str) -> tuple[float, float]:
        location = location.strip()
        if "," in location and any(c.isdigit() for c in location.split(",")[0]):
            # Looks like "lat,lon" already
            try:
                lat_str, lon_str = location.split(",")
                return float(lat_str.strip()), float(lon_str.strip())
            except ValueError:
                pass
        key = location.lower()
        if key not in _CITY_COORDS:
            raise ValueError(
                f"Unknown location '{location}'. Pass 'lat,lon' directly, "
                f"or add it to _CITY_COORDS in weather_client.py."
            )
        return _CITY_COORDS[key]

    def get_grid_point(self, location: str) -> dict:
        """Resolve a location to its NWS forecast office + grid x/y."""
        lat, lon = self._resolve_latlon(location)
        resp = self.session.get(f"{self.base_url}/points/{lat},{lon}")
        resp.raise_for_status()
        props = resp.json()["properties"]
        return {
            "office": props["gridId"],
            "grid_x": props["gridX"],
            "grid_y": props["gridY"],
            "state": props.get("relativeLocation", {})
                .get("properties", {})
                .get("state"),
        }

    # -- raw fetches -----------------------------------------------------------
    def get_active_alerts(self, state: str) -> list[dict]:
        """Active alerts for a US state abbreviation, e.g. 'IL'."""
        resp = self.session.get(f"{self.base_url}/alerts/active", params={"area": state})
        resp.raise_for_status()
        return resp.json().get("features", [])

    def get_forecast(self, office: str, grid_x: int, grid_y: int) -> list[dict]:
        """Multi-day narrative forecast periods for a grid point."""
        resp = self.session.get(
            f"{self.base_url}/gridpoints/{office}/{grid_x},{grid_y}/forecast"
        )
        resp.raise_for_status()
        return resp.json().get("properties", {}).get("periods", [])

    # -- normalization -----------------------------------------------------
    def get_documents_for_location(self, location: str, limit: int = 50) -> list[dict]:
        """
        Fetch alerts + forecast for one location and normalize both into a
        flat list of document dicts ready for the weather_documents table.
        """
        grid = self.get_grid_point(location)
        docs = []

        if grid.get("state"):
            for alert in self.get_active_alerts(grid["state"]):
                docs.append(self._normalize_alert(location, alert))

        for period in self.get_forecast(grid["office"], grid["grid_x"], grid["grid_y"]):
            docs.append(self._normalize_forecast_period(location, period))

        return docs[:limit]

    @staticmethod
    def _normalize_alert(location: str, feature: dict) -> dict:
        props = feature.get("properties", {})
        narrative = " ".join(
            filter(None, [props.get("description"), props.get("instruction")])
        ).strip()
        return {
            "id": props.get("id") or feature.get("id"),
            "location": location,
            "source_type": "alert",
            "headline": props.get("event", "Weather Alert"),
            "narrative_text": narrative or props.get("headline", ""),
            "issued_at": props.get("sent"),
            "effective_at": props.get("effective"),
            "payload": feature,
        }

    @staticmethod
    def _normalize_forecast_period(location: str, period: dict) -> dict:
        raw_key = f"{location}|{period.get('startTime')}|{period.get('name')}"
        stable_id = "forecast-" + hashlib.sha1(raw_key.encode("utf-8")).hexdigest()[:16]
        return {
            "id": stable_id,
            "location": location,
            "source_type": "forecast",
            "headline": period.get("name", "Forecast"),
            "narrative_text": period.get("detailedForecast", ""),
            "issued_at": period.get("startTime"),
            "effective_at": period.get("endTime"),
            "payload": period,
        }
