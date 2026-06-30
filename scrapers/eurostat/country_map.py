"""
27 EU member state metadata: ISO3 ↔ Eurostat geo code mapping.

Key difference: Greece uses "EL" in Eurostat (not "GR").
All other countries match ISO 3166-1 alpha-2.
"""

# (iso3, eurostat_geo2, country_name, market_tier)
EU27: list[tuple] = [
    ("AUT", "AT", "Austria",     "Developed"),
    ("BEL", "BE", "Belgium",     "Developed"),
    ("BGR", "BG", "Bulgaria",    "Developed"),
    ("HRV", "HR", "Croatia",     "Developed"),
    ("CYP", "CY", "Cyprus",      "Developed"),
    ("CZE", "CZ", "Czechia",     "Developed"),
    ("DNK", "DK", "Denmark",     "Developed"),
    ("EST", "EE", "Estonia",     "Developed"),
    ("FIN", "FI", "Finland",     "Developed"),
    ("FRA", "FR", "France",      "Developed"),
    ("DEU", "DE", "Germany",     "Developed"),
    ("GRC", "EL", "Greece",      "Developed"),   # EL, not GR
    ("HUN", "HU", "Hungary",     "Developed"),
    ("IRL", "IE", "Ireland",     "Developed"),
    ("ITA", "IT", "Italy",       "Developed"),
    ("LVA", "LV", "Latvia",      "Developed"),
    ("LTU", "LT", "Lithuania",   "Developed"),
    ("LUX", "LU", "Luxembourg",  "Developed"),
    ("MLT", "MT", "Malta",       "Developed"),
    ("NLD", "NL", "Netherlands", "Developed"),
    ("POL", "PL", "Poland",      "Developed"),
    ("PRT", "PT", "Portugal",    "Developed"),
    ("ROU", "RO", "Romania",     "Developed"),
    ("SVK", "SK", "Slovakia",    "Developed"),
    ("SVN", "SI", "Slovenia",    "Developed"),
    ("ESP", "ES", "Spain",       "Developed"),
    ("SWE", "SE", "Sweden",      "Developed"),
]

# Lookup tables
ISO3_TO_GEO2:  dict[str, str] = {r[0]: r[1] for r in EU27}
GEO2_TO_ISO3:  dict[str, str] = {r[1]: r[0] for r in EU27}
ISO3_TO_NAME:  dict[str, str] = {r[0]: r[2] for r in EU27}
ISO3_TO_TIER:  dict[str, str] = {r[0]: r[3] for r in EU27}

ALL_ISO3:  list[str] = [r[0] for r in EU27]
ALL_GEO2:  list[str] = [r[1] for r in EU27]
