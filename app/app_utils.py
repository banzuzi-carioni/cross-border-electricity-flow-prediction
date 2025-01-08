COUTRY_TO_COORDS = {
    'NL': (52.25, 5.54),
    'GB': (54.7, -3.28),
    'NO_2': (61.15, 8.79),
    'DK_1': (55.67, 10.33),
    'DE_LU': (50.93, 6.94),
    'BE': (50.64, 4.67)
}

def get_country_center_coordinates(country_code: str):
    """
    Takes city name and returns its latitude and longitude (rounded to 2 digits after dot).
    """
    latitude, longitude = COUTRY_TO_COORDS.get(country_code, (0, 0))
    return longitude, latitude
