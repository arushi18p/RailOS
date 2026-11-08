"""
Predefined train routes used by the simulator.

Each route is a named list of (lat, lon, stop_name) waypoints. In a real
system these would come from a GTFS static feed; here they're hand-picked
points along real-ish corridors so the map demo looks plausible.
"""

ROUTES = {
    "NEC-101": {
        "name": "Northeast Corridor Express",
        "stops": [
            {"name": "Boston South", "lat": 42.3519, "lon": -71.0552},
            {"name": "Providence", "lat": 41.8290, "lon": -71.4128},
            {"name": "New Haven", "lat": 41.2967, "lon": -72.9273},
            {"name": "New York Penn", "lat": 40.7506, "lon": -73.9935},
            {"name": "Philadelphia 30th St", "lat": 39.9566, "lon": -75.1819},
            {"name": "Baltimore Penn", "lat": 39.3079, "lon": -76.6156},
            {"name": "Washington Union", "lat": 38.8973, "lon": -77.0063},
        ],
    },
    "CAP-220": {
        "name": "Capitol Limited",
        "stops": [
            {"name": "Washington Union", "lat": 38.8973, "lon": -77.0063},
            {"name": "Harpers Ferry", "lat": 39.3253, "lon": -77.7386},
            {"name": "Cumberland", "lat": 39.6528, "lon": -78.7625},
            {"name": "Pittsburgh", "lat": 40.4518, "lon": -80.0018},
            {"name": "Cleveland", "lat": 41.4993, "lon": -81.6944},
            {"name": "Toledo", "lat": 41.6528, "lon": -83.5379},
            {"name": "Chicago Union", "lat": 41.8789, "lon": -87.6359},
        ],
    },
    "PSW-501": {
        "name": "Pacific Surfliner",
        "stops": [
            {"name": "San Luis Obispo", "lat": 35.2828, "lon": -120.6596},
            {"name": "Santa Barbara", "lat": 34.4147, "lon": -119.6982},
            {"name": "Los Angeles Union", "lat": 34.0559, "lon": -118.2345},
            {"name": "Anaheim", "lat": 33.8093, "lon": -117.8827},
            {"name": "Oceanside", "lat": 33.1958, "lon": -117.3795},
            {"name": "San Diego Santa Fe", "lat": 32.7157, "lon": -117.1668},
        ],
    },
}
