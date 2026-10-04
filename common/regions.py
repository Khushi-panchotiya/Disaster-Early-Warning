"""Region dictionary: region -> (lat, lon, place names). Used for tagging, the map and the simulator."""
import re

REGIONS = {
    "Assam": (26.2, 92.9, ["Guwahati", "Dibrugarh", "Brahmaputra valley", "Silchar"]),
    "Kerala": (10.5, 76.3, ["Kochi", "Wayanad", "Thiruvananthapuram", "Kozhikode"]),
    "Maharashtra": (19.7, 75.7, ["Mumbai", "Pune", "Nagpur", "Nashik"]),
    "Tamil Nadu": (11.1, 78.7, ["Chennai", "Madurai", "Coimbatore"]),
    "Delhi": (28.6, 77.2, ["New Delhi", "Noida", "Gurgaon"]),
    "Uttarakhand": (30.1, 79.3, ["Dehradun", "Chamoli", "Kedarnath", "Haridwar"]),
    "Himachal Pradesh": (31.9, 77.2, ["Shimla", "Manali", "Kullu"]),
    "Gujarat": (22.3, 71.8, ["Ahmedabad", "Surat", "Kutch"]),
    "West Bengal": (23.0, 87.9, ["Kolkata", "Siliguri", "Sundarbans"]),
    "Odisha": (20.5, 85.1, ["Bhubaneswar", "Cuttack", "Puri"]),
    "Bihar": (25.6, 85.1, ["Patna", "Gaya", "Kosi"]),
    "Karnataka": (15.3, 75.7, ["Bengaluru", "Bangalore", "Mysuru", "Mangaluru"]),
    "California": (36.8, -119.4, ["Los Angeles", "San Diego", "Sonoma", "San Francisco"]),
    "Texas": (31.0, -100.0, ["Houston", "Dallas", "Austin"]),
    "Florida": (27.7, -81.5, ["Miami", "Tampa", "Orlando"]),
    "Japan": (36.2, 138.3, ["Tokyo", "Osaka", "Fukushima", "Hokkaido"]),
    "Nepal": (28.4, 84.1, ["Kathmandu", "Pokhara", "Gorkha"]),
    "Turkey": (39.0, 35.0, ["Istanbul", "Ankara", "Izmir", "Hatay"]),
    "Indonesia": (-2.5, 118.0, ["Jakarta", "Sumatra", "Java", "Sulawesi"]),
    "Australia": (-25.3, 133.8, ["Sydney", "Melbourne", "Queensland", "New South Wales"]),
}
UNKNOWN = "Unknown"

_PATTERNS = [
    (name, [re.compile(rf"\b{re.escape(t)}\b", re.IGNORECASE) for t in [name, *places]])
    for name, (_lat, _lon, places) in REGIONS.items()
]


def tag_region(text):
    """Return the region with the most distinct name/place hits in text, else 'Unknown'."""
    best, best_hits = UNKNOWN, 0
    for name, pats in _PATTERNS:
        hits = sum(1 for p in pats if p.search(text or ""))
        if hits > best_hits:
            best, best_hits = name, hits
    return best


def places(region):
    return REGIONS[region][2]


def coords(region):
    lat, lon, _ = REGIONS[region]
    return lat, lon
